package openpolitica.data.congreso;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pe.congreso.leyes.Congresista;
import pe.congreso.leyes.Documento;
import pe.congreso.leyes.Enlaces;
import pe.congreso.leyes.Expediente;
import pe.congreso.leyes.Ley;
import pe.congreso.leyes.ProyectoLey;
import pe.congreso.leyes.Seguimiento;

import static java.util.stream.Collectors.toList;

public class ProyectosLeyExtract {

  static final Logger LOG = LoggerFactory.getLogger(ProyectosLeyExtract.class);
  static final Pattern datePattern = Pattern.compile("\\d{2}/\\d{2}/\\d{4}");

  final String baseUrl;
  final String proyectosUrl;
  final String expedienteUrl;
  final int maxBatchSize;

  public ProyectosLeyExtract(
      String baseUrl,
      String proyectosUrl,
      String expedienteUrl,
      int maxBatchSize
  ) {
    this.baseUrl = baseUrl;
    this.proyectosUrl = proyectosUrl;
    this.expedienteUrl = expedienteUrl;
    this.maxBatchSize = maxBatchSize;
  }

  ArrayList<ProyectoLey> run() {
    LOG.info("Iniciando extraccion");
    var index = 1;
    var batchSize = 0;

    var proyectos = new ArrayList<ProyectoLey>();

    do {
      var pagina =
          Retry.decorateFunction(
              Retry.of("importar-proyectos", RetryConfig.custom()
                  .maxAttempts(3)
                  .retryExceptions(RuntimeException.class)
                  .waitDuration(Duration.ofSeconds(10))
                  .build()),
              this::importarPagina)
              .apply(index);

      proyectos.addAll(pagina.entrySet().parallelStream()
          .map(entry ->
              Retry.decorateFunction(
                  Retry.of("importar-seguimiento", RetryConfig.custom()
                      .maxAttempts(3)
                      .retryExceptions(RuntimeException.class)
                      .waitDuration(Duration.ofSeconds(10))
                      .build()),
                  this::importarSeguimiento)
                  .apply(entry.getValue()))
          .parallel()
          .map(p ->
              Retry.decorateFunction(
                  Retry.of("importar-expediente", RetryConfig.custom()
                      .maxAttempts(3)
                      .retryExceptions(RuntimeException.class)
                      .waitDuration(Duration.ofSeconds(10))
                      .build()),
                  this::importarExpediente)
                  .apply(p))
          .collect(toList()));

      batchSize = pagina.size();
      index = index + batchSize;

      LOG.info("Proyectos importados: {}", index);
    } while (batchSize == maxBatchSize);

    proyectos.sort(Comparator.comparing(ProyectoLey::getPeriodoNumero));
    return proyectos;
  }

  void save(Path output, List<ProyectoLey> solicitudes) throws IOException {
    var datumWriter = new SpecificDatumWriter<>(ProyectoLey.class);
    try (var writer = new DataFileWriter<>(datumWriter)) {
      writer.setCodec(CodecFactory.zstandardCodec(CodecFactory.DEFAULT_ZSTANDARD_LEVEL));
      writer.create(ProyectoLey.getClassSchema(), output.toFile());

      solicitudes.forEach(s -> {
        try {
          if (s != null) {
            writer.append(s);
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
    }
  }

  Map<String, Map<String, Object>> importarPagina(int index) {
    try {
      var url = baseUrl + proyectosUrl + index;
      var doc = Jsoup.connect(url).get();
      var tablas = doc.body().getElementsByTag("table");
      if (tablas.size() != 3) {
        LOG.error("Numero de tablas inesperado: {}, url={}", tablas.size(), url);
        throw new IllegalStateException("Unexpected number of tables");
      }
      var proyectos = new LinkedHashMap<String, Map<String, Object>>();
      var filas = tablas.get(1).getElementsByTag("tr");
      for (int i = 1; i < filas.size(); i++) {
        var proyecto = mapProyecto(filas.get(i));
        proyectos.put(proyecto.get("numero").toString(), proyecto);
      }
      return proyectos;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  Map<String, Object> mapProyecto(Element row) {
    var campos = row.getElementsByTag("td");
    if (campos.size() != 5) {
      LOG.error("Numero inesperado de campos: {}, fila: {}", campos.size(), row.html());
      throw new IllegalStateException("Numero inesperado de campos");
    }
    var numero = campos.get(0).text();
    var fechaActualizacion = campos.get(1).text().isBlank() ?
        null :
        fechaProyecto(campos.get(1).text().trim());
    var fechaPresentacion = fechaProyecto(campos.get(2).text().trim());
    var estado = campos.get(3).text();
    var titulo = campos.get(4).text()
        .replaceAll("\"\"", "\"")
        .replaceAll("\"", "'")
        .replaceAll(",,", ",")
        .replaceAll(":", ".-");
    var enlaceSeguimiento = baseUrl + campos.get(0).getElementsByTag("a").attr("href");
    var proyecto = new HashMap<String, Object>();
    proyecto.put("numero", numero);
    //proyecto.put("periodo", periodo);
    proyecto.put("estado", estado);
    proyecto.put("titulo", titulo);
    proyecto.put("publicacion_fecha", fechaPresentacion);
    proyecto.put("actualizacion_fecha", fechaActualizacion);
    proyecto.put("enlace_seguimiento", enlaceSeguimiento);
    return proyecto;
  }

  private Long fechaProyecto(String texto) {
    return LocalDate.parse(texto, DateTimeFormatter.ofPattern("MM/dd/yyyy"))
        .atStartOfDay()
        .toInstant(ZoneOffset.ofHours(-5))
        .toEpochMilli();
  }

  ProyectoLey.Builder importarSeguimiento(Map<String, Object> importado) {
    var url = (String) importado.get("enlace_seguimiento");
    try {
      var doc = Jsoup.connect(url).get();
      var scripts = doc.head().getElementsByTag("script");
      if (scripts.size() != 2) {
        LOG.error("Numero inesperado de scripts {}, url={}", scripts.size(), url);
        throw new IllegalStateException("Unexpected number of tables");
      }
      var tablas = doc.body().getElementsByTag("table");
      if (tablas.size() != 2) {
        LOG.error("Unexpected number of tables url={}", url);
        throw new IllegalStateException("Unexpected number of tables");
      }

      var urlExpediente = String.format(baseUrl + expedienteUrl, importado.get("numero"));

      var ley = Ley.newBuilder();

      var builder = ProyectoLey.newBuilder();

      builder.setIniciativasAgrupadas(List.of());
      builder.setAdherentes(List.of());

      var contenidoTabla = tablas.get(1);
      contenidoTabla.getElementsByTag("tr")
          .forEach(tr -> {
            var tds = tr.getElementsByTag("td");
            var field = tds.get(0).text();
            var entry = tds.get(1);
            mapSeguimiento(builder, ley, field, entry);
            if (tds.size() == 4) {
              mapSeguimiento(builder, ley, tds.get(2).text(), tds.get(3));
            }
          });

      var seguimientos = new ArrayList<Seguimiento>();
      if (builder.getSeguimientoTexto() != null && !builder.getSeguimientoTexto().isBlank()) {
        var matcher = datePattern.matcher(builder.getSeguimientoTexto());
        var textos = Arrays.stream(builder.getSeguimientoTexto().split(datePattern.pattern()))
            .map(String::trim)
            .filter(s -> !s.isBlank())
            .collect(toList());
        for (String texto : textos) {
          if (matcher.find()) {
            var fecha = matcher.group();
            seguimientos.add(Seguimiento.newBuilder()
                .setEvento(texto)
                .setFecha(fecha(fecha))
                .build());
          }
        }
      }

      var prefix = "Decretado a...";
      var sectores = new ArrayList<String>();
      for (Seguimiento s : seguimientos) {
        if (s.getEvento().startsWith(prefix)) {
          final var sector = s.getEvento().substring(prefix.length() + 1).strip();
          if (sector.contains("-")) {
            var corregido = sector.substring(0, sector.indexOf("-"));
            sectores.add(corregido);
          } else {
            sectores.add(sector);
          }
        }
      }

      if (builder.getTitulo() == null) builder.setTitulo((String) importado.get("titulo"));

      builder
          .setPeriodoNumero((String) importado.get("numero"))
          .setEstado((String) importado.get("estado"))
          .setActualizacionFecha((Long) importado.get("actualizacion_fecha"))
          .setPublicacionFecha((Long) importado.get("publicacion_fecha"))
          .setSectores(sectores)
          .setSeguimiento(seguimientos)
          .setLey(ley.getNumero() == null ? null : ley.build())
          .setEnlacesBuilder(Enlaces.newBuilder()
              .setExpediente(urlExpediente)
              .setOpinionesPublicadas(null)
              .setOpinionesPublicar(null)
              .setSeguimiento(url));
      return builder;
    } catch (HttpStatusException e) {
      if (e.getStatusCode() == 404) {
        LOG.error("Error procesando proyecto {} referencia {}. Pagina no existe!!!",
            importado, url);
        return null;
      }
      LOG.error("Error procesando proyecto {} referencia {}", importado, url);
      throw new RuntimeException(e);
    } catch (Throwable e) {
      LOG.error("Error procesando proyecto {} referencia {}", importado, url);
      throw new RuntimeException(e);
    }
  }

  private void mapSeguimiento(ProyectoLey.Builder builder, Ley.Builder ley, String field,
      Element entry) {
    var texto = entry.text().trim();
    switch (field) {
      case "Período:" -> builder.setPeriodo(texto);
      case "Legislatura:", "Legislatura:." -> builder.setLegislatura(texto);
      case "Número:" -> builder.setNumeroUnico(texto);
      case "Fecha Presentación:" -> {
      }
      case "Proponente:" -> builder.setProponente(texto);
      case "Grupo Parlamentario:" -> {
        if (!texto.isBlank()) {
          builder.setGrupoParlamentario(texto);
        }
      }
      case "Título:" -> builder.setTitulo(texto
          .replaceAll("\"\"", "\"")
          .replaceAll("\"", "'")
          .replaceAll(",,", ",")
          .replaceAll(":", ".-"));
      case "Sumilla:" -> {
        if (!texto.isBlank()) {
          builder.setSumilla(texto);
        }
      }
      case "Seguimiento:", "Seguimiento:." -> builder.setSeguimientoTexto(texto);
      case "Autores (*):" -> builder.setAutores(autores(entry));
      case "Adherentes(**):" -> builder.setAdherentes(adherentes(entry));
      case "Iniciativas Agrupadas:" -> {
        if (!texto.isBlank()) {
          var values = Arrays.stream(texto.split(","))
              .map(String::trim)
              .collect(toList());
          builder.setIniciativasAgrupadas(values);
        }
      }
      case "Número de Ley:" -> ley.setNumero(texto);
      case "Título de la Ley:" -> ley.setTitulo(texto);
      case "Sumilla de la Ley" -> {
        if (!texto.isBlank()) {
          ley.setSumilla(texto);
        }
      }
      default -> LOG.error("Campo no mapeado: " + field);
    }
  }

  private List<Congresista> autores(Element element) {
    return
        element.getElementsByTag("a").stream()
            .map(a -> {
              String email = a.attr("href");
              String nombreCompleto = a.text();
              return Congresista.newBuilder()
                  .setCorreoElectronico(email)
                  .setNombreCompleto(nombreCompleto)
                  .build();
            })
            .collect(toList());
  }

  private List<String> adherentes(Element element) {
    return Arrays.asList(element.text().split(","));
  }

  private Long fecha(String texto) {
    return LocalDate.parse(texto
            .replaceAll("58/08/2018", "08/08/2018")
            .replaceAll("59/02/2017", "06/02/2017")
            .replaceAll("60/02/2017", "06/02/2017")
            .replaceAll("61/02/2017", "06/02/2017")
            .replaceAll("62/02/2017", "06/02/2017")
            // 2011
            .replaceAll("35/07/2014", "25/07/2014")
        , DateTimeFormatter.ofPattern("dd/MM/yyyy"))
        .atStartOfDay()
        .toInstant(ZoneOffset.ofHours(-5))
        .toEpochMilli();
  }

  ProyectoLey importarExpediente(ProyectoLey.Builder builder) {
    var enlaceExpediente = builder.getEnlacesBuilder().getExpediente();
    try {
      var doc = Jsoup.connect(enlaceExpediente).get();
      var expedienteBuilder = mapExpediente(doc);
      builder.setExpediente(expedienteBuilder.build());

      //extrayendo opiniones
      var enlacesBuilder = builder.getEnlacesBuilder();
      mapEnlacesOpiniones(doc, enlacesBuilder);

      return builder.build();
    } catch (HttpStatusException e) {
      if (e.getStatusCode() == 404) {
        LOG.warn("Error procesando expediente {}, no encontrado", enlaceExpediente);
        return builder.setExpediente(null).build();
      } else {
        LOG.error("Error procesando expediente {}", enlaceExpediente, e);
        throw new RuntimeException(e);
      }
    } catch (Throwable e) {
      LOG.error("Error procesando expediente {}", enlaceExpediente, e);
      throw new RuntimeException(e);
    }
  }

  private static void mapEnlacesOpiniones(Document doc, Enlaces.Builder enlacesBuilder) {
    var exps = doc.body().select("td[width=173]").stream()
        .filter(e -> e.getElementsByTag("table").size() > 0)
        .collect(toList());
    if (exps.size() == 2) {
      var presentarOpinionUrl = enlacePresentarOpinion(doc, exps.get(0));
      enlacesBuilder.setOpinionesPublicar(presentarOpinionUrl);
      var opinionesUrl = enlaceOpinionesPresentadas(doc);
      enlacesBuilder.setOpinionesPublicadas(opinionesUrl);
    }
    if (exps.size() == 1) {
      var opinionesUrl = enlaceOpinionesPresentadas(doc);
      enlacesBuilder.setOpinionesPublicadas(opinionesUrl);
    }
  }

  private Expediente.Builder mapExpediente(Document doc) {
    var tds = doc.body().select("td[width=552]");
    var main = tds.size() > 1 ? tds.last() : tds.first();
    var justify = main.select("div[align=justify]").first();
    Element center;
    if (justify == null) {
      center = doc.select("div[align=justify]").first();
    } else {
      center = justify.select("div[align=center]").first();
    }

    var titulos = center.select("b");
    var expedienteBuilder = Expediente.newBuilder();
    if (!titulos.isEmpty()) {
      expedienteBuilder.setTitulo(titulos.get(0).text());
    }
    if (titulos.size() > 1) {
      var titulo = titulos.get(1).text();
      expedienteBuilder.setSubtitulo(titulo);
    }

    //extrayendo documentos
    expedienteBuilder.setDocumentos(List.of());
    var tablasDocumento = center.getElementsByTag("table");
    //  cuando contiene docs de ley
    if (tablasDocumento.size() == 3) {
      var docsResultado = documentos("RESULTADO", tablasDocumento.first());
      var docsProyecto = documentos("PROYECTO", tablasDocumento.get(1));
      var docsAnexos = documentos("ANEXOS", tablasDocumento.get(2));
      expedienteBuilder.setDocumentos(docsResultado);
      expedienteBuilder.getDocumentos().addAll(docsProyecto);
      expedienteBuilder.getDocumentos().addAll(docsAnexos);
    }
    //  cuando solo contiene proyecto y anexos
    if (tablasDocumento.size() == 2) {
      var docsProyecto = documentos("PROYECTO", tablasDocumento.get(0));
      var docsAnexos = documentos("ANEXOS", tablasDocumento.get(1));
      expedienteBuilder.setDocumentos(docsProyecto);
      expedienteBuilder.getDocumentos().addAll(docsAnexos);
    }
    //  cuando solo contiene docs de proyecto
    if (tablasDocumento.size() == 1) {
      var docsProyecto = documentos("PROYECTO", tablasDocumento.get(0));
      expedienteBuilder.setDocumentos(docsProyecto);
    }
    return expedienteBuilder;
  }

  private static String enlaceOpinionesPresentadas(Document doc) {
    var scripts = doc.head().getElementsByTag("script");
    var html = scripts.get(0).html();
    var enlace = Arrays.stream(html.split("\\r"))
        .filter(s -> s.strip().startsWith("window.open"))
        .findFirst()
        .map(s -> s.substring(s.indexOf("(") + 1, s.lastIndexOf(")")))
        .map(s -> s.split(",")[0])
        .map(link -> {
          var urlPattern = link.substring(
              link.indexOf("\"") + 1,
              link.lastIndexOf("\"")
          );
          var idElement = doc.select("input[name=IdO]");
          var value = idElement.first().attr("value");
          return urlPattern.replace("\" + num + \"", value);
        });
    if (enlace.isEmpty()) {
      LOG.warn("Enlace de opiniones presentadas no ha sido encontrado {}", html);
      return null;
    } else {
      return enlace.get();
    }
  }

  private static String enlacePresentarOpinion(Document doc, Element opinionTable) {
    var onclick = opinionTable.getElementsByTag("a").attr("onclick");
    var ruta = onclick.indexOf("ruta3 =") + 7;
    var link = onclick.substring(ruta, onclick.indexOf(";", ruta));
    var urlPattern = link.substring(
        link.indexOf("\"") + 1,
        link.lastIndexOf("\"")
    );
    var idElement = doc.select("input[name=IdO]");
    var value = idElement.first().attr("value");
    return urlPattern.replace("\"+ids+\"", value);
  }

  private List<Documento> documentos(String tipo, Element table) {
    try {
      var rows = table.getElementsByTag("tr");
      var th = rows.first().getElementsByTag("th");
      var td = rows.first().getElementsByTag("td");
      var headers = rows.first().getElementsByTag("b");
      //extraer documentos de ley
      if (th.size() == 3 || headers.size() == 5 || td.size() == 3) {
        var docs = new ArrayList<Documento>();
        for (int i = 1; i < rows.size(); i++) {
          var row = rows.get(i);
          var values = row.getElementsByTag("td");
          if (values.size() == 3) {
            var element = values.get(2);
            var nombreDocumento = element.text();
            var referenciaDocumento = element.getElementsByTag("a").attr("href");
            var fecha = fecha(values.get(1));
            var builder = Documento.newBuilder()
                .setTitulo(nombreDocumento)
                .setUrl(referenciaDocumento)
                .setTipo(tipo)
                .setFecha(fecha);
            var doc = builder.build();
            docs.add(doc);
          } else if (values.size() == 1) {
            var element = values.get(0);
            var referenciaDocumento = element.getElementsByTag("a").attr("href");
            var doc = Documento.newBuilder()
                .setTitulo(null)
                .setUrl(referenciaDocumento)
                .setTipo(tipo)
                .setFecha(null)
                .build();
            docs.add(doc);
          } else {
            LOG.warn("Numero de columnas no esperado {}", values.size());
          }
        }
        return docs;
      } else if (th.size() == 2 || headers.size() == 2) { //extraer documentos de proyecto
        var docs = new ArrayList<Documento>();
        for (int i = 1; i < rows.size(); i++) {
          var row = rows.get(i);
          var values = row.getElementsByTag("td");
          var element = values.get(1);
          var nombreDocumento = element.text();
          var referenciaDocumento = element.getElementsByTag("a").attr("href");
          var fecha = fecha(values.get(0));

          var builder = Documento.newBuilder()
              .setTitulo(nombreDocumento)
              .setUrl(referenciaDocumento)
              .setTipo(tipo)
              .setFecha(fecha);
          var doc = builder.build();
          docs.add(doc);
        }
        return docs;
      } else if (th.size() == 0) { //extraer documentos de anexos
        var docs = new ArrayList<Documento>();
        var start = 0;
        if (headers.size() > 0) {
          start = 1;
        }
        for (int i = start; i < rows.size(); i++) {
          var row = rows.get(i);
          var values = row.getElementsByTag("td");
          var element = values.get(1);
          var nombreDocumento = element.text();
          var referenciaDocumento = element.getElementsByTag("a").attr("href");
          var fecha = fecha(values.get(0));
          var builder = Documento.newBuilder()
              .setTitulo(nombreDocumento)
              .setUrl(referenciaDocumento)
              .setTipo(tipo)
              .setFecha(fecha);
          var doc = builder.build();
          docs.add(doc);
        }
        return docs;
      } else {
        LOG.error("Numero de columnas {} y cabeceras {} no es esperado. \n {}",
            th.size(), headers.size(), table.html());
        throw new IllegalStateException("Numero de cabeceras de documentos inespeado");
      }
    } catch (Throwable e) {
      LOG.error("Error obteniendo documentos {}", table.html(), e);
      throw new IllegalStateException("Error obteniendo documentos", e);
    }
  }

  private Long fecha(Element td) {
    if (td.text().isBlank()) {
      //LOG.warn("Fecha vacia! {}", td.html());
      return null;
    }
    //agregar cualquier condicion para arreglar inconsistencias en fechas
    if (td.text().length() == 10) {
      return LocalDate.parse(td.text(),
          DateTimeFormatter.ofPattern("dd/MM/yyyy"))
          .atStartOfDay()
          .toInstant(ZoneOffset.ofHours(-5))
          .toEpochMilli();
    } else {
      if (td.text().length() == 8) {
        return LocalDate.parse(td.text()
                .replaceAll("\\s+", "")
                .replaceAll("-", "")
                .replaceAll("\\+", "")
                .replaceAll("//", "/")
                .replaceAll("02/15/19", "15/02/19")
                .replaceAll("20/0708", "20/07/18")
            ,
            DateTimeFormatter.ofPattern("dd/MM/yy"))
            .atStartOfDay()
            .toInstant(ZoneOffset.ofHours(-5))
            .toEpochMilli();
      } else {
        if (td.text().length() > 10 || td.text().length() < 6) {
          return null;
        } else {
          return LocalDate.parse(td.text()
                  .replaceAll("\\s+", "")
                  .replaceAll("-", "")
                  .replaceAll("\\+", "")
                  .replaceAll("//", "/")
                  .replaceAll("011", "11")
                  .replaceAll("119", "19")
                  .replaceAll("240", "24")
                  .replaceAll("178", "18")
                  .replaceAll("187", "18")
                  .replaceAll("182", "18")
                  .replaceAll("0520", "05/20")
                  .replaceAll("5/04/19", "05/04/19")
                  .replaceAll("0719", "07/19")
                  .replaceAll("0617", "06/17")
                  .replaceAll("1710", "17/10")
                  .replaceAll("1018", "10/18")
                  .replaceAll("0208", "02/08")
                  .replaceAll("1907", "19/07")
                  .replaceAll("23/03/18/", "23/03/18")
                  .replaceAll("02/15/19", "15/02/19")
                  .replaceAll("21/5/20", "21/05/20")
                  .replaceAll("20/0708", "20/07/18")
                  .replaceAll("21/012/06", "21/12/06")
                  .replaceAll("12/142/06", "12/12/06")
                  .replaceAll("15/0307", "15/03/07")
                  .replaceAll("21/1206", "21/12/06")
                  .replaceAll("029/06/06", "29/06/06")
              ,
              DateTimeFormatter.ofPattern("dd/MM/yy"))
              .atStartOfDay()
              .toInstant(ZoneOffset.ofHours(-5))
              .toEpochMilli();
        }
      }
    }
  }

  public static void main(String[] args) throws IOException {
    var doc = Jsoup.connect(
        "http://www2.congreso.gob.pe/Sicr/TraDocEstProc/Expvirt_2011.nsf/visbusqptramdoc1621/03301?opendocument"
        //"http://www2.congreso.gob.pe/sicr/tradocestproc/Expvirt_2011.nsf/visbusqptramdoc1621/06611?opendocument"
    )
        .get();
    ProyectosLeyExtract.mapEnlacesOpiniones(doc,
        Enlaces.newBuilder()
            .setExpediente("exp")
            .setSeguimiento("seg"));
  }
}
