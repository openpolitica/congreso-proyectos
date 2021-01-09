package openpolitica.congreso;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import openpolitica.congreso.leyes.ProyectoLey;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProyectosLeyLoadSqlite {
  static final Logger LOG = LoggerFactory.getLogger(ProyectosLeyLoadSqlite.class);

  static List<TableLoad> tableLoadList = List.of(
      new ProyectoTableLoad(),
      new IniciativaAgrupadaTableLoad(),
      new CongresistaTableLoad(),
      new AdherenteTableLoad(),
      new AutorTableLoad(),
      new SectoresTableLoad(),
      new SeguimientoTableLoad(),
      new ExpedienteTableLoad()
  );

  DataFileReader<ProyectoLey> load(Path input) throws IOException {
    var reader = new SpecificDatumReader<>(ProyectoLey.class);
    return new DataFileReader<>(input.toFile(), reader);
  }

  public void save(Path input, Path output) throws SQLException, IOException {
    var jdbcUrl = "jdbc:sqlite:" + output.toAbsolutePath();
    try (var connection = DriverManager.getConnection(jdbcUrl)) {
      var statement = connection.createStatement();
      for (var tableLoad : tableLoadList) {
        LOG.info("Loading {}", tableLoad.tableName);
        statement.executeUpdate(tableLoad.dropTableStatement());
        statement.executeUpdate(tableLoad.createTableStatement());
        LOG.info("Table {} created", tableLoad.tableName);

        var ps = connection.prepareStatement(tableLoad.prepareStatement());
        LOG.info("Statement for {} prepared", tableLoad.tableName);

        var reader = load(input);
        while (reader.hasNext()) tableLoad.addBatch(ps, reader.next());
        LOG.info("Batch for {} ready", tableLoad.tableName);
        ps.executeBatch();
        LOG.info("Table {} updated", tableLoad.tableName);
      }
    }
  }

  abstract static class TableLoad {
    final String tableName;

    public TableLoad(String tableName) {
      this.tableName = tableName;
    }

    String dropTableStatement() {
      return "drop table if exists %s".formatted(tableName);
    }

    abstract String createTableStatement();

    abstract String prepareStatement();

    abstract void addBatch(PreparedStatement ps, ProyectoLey pl) throws SQLException;
  }

  static class ProyectoTableLoad extends TableLoad {
    public ProyectoTableLoad() {
      super("proyecto");
    }

    String createTableStatement() {
      return """
          create table %s (
            periodo string,
            periodo_numero string,
            estado string,
            publicacion_fecha long,
            actualizacion_fecha long,
            numero_unico string primary key,
            titulo string,
            sumilla string,
            legislatura string,
            proponente string,
            grupo_parlamentario string,
            seguimiento_texto string,
            expediente_titulo string,
            expediente_subtitulo string,
            ley_numero string,
            ley_titulo string,
            ley_sumilla string,     
            enlace_seguimiento string,
            enlace_expediente string,
            enlace_opiniones_publicadas string,
            enlace_opiniones_publicar string
          )
          """.formatted(tableName);
    }

    String prepareStatement() {
      return """
          insert into %s values (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?
          )
          """.formatted(tableName);
    }

    void addBatch(PreparedStatement ps, ProyectoLey pl) throws SQLException {
      ps.setString(1, pl.getPeriodo());
      ps.setString(2, pl.getPeriodoNumero());
      ps.setString(3, pl.getEstado());
      ps.setLong(4, pl.getPublicacionFecha());
      if (pl.getActualizacionFecha() != null) ps.setLong(5, pl.getActualizacionFecha());
      ps.setString(6, pl.getNumeroUnico());
      ps.setString(7, pl.getTitulo());
      ps.setString(8, pl.getSumilla());
      ps.setString(9, pl.getLegislatura());
      ps.setString(10, pl.getProponente());
      ps.setString(11, pl.getGrupoParlamentario());
      ps.setString(12, pl.getSeguimientoTexto());
      if (pl.getExpediente() != null) {
        ps.setString(13, pl.getExpediente().getTitulo());
        ps.setString(14, pl.getExpediente().getSubtitulo());
      }
      if (pl.getLey() != null) {
        ps.setString(15, pl.getLey().getNumero());
        ps.setString(16, pl.getLey().getTitulo());
        ps.setString(17, pl.getLey().getSumilla());
      }
      ps.setString(18, pl.getEnlaces().getSeguimiento());
      ps.setString(19, pl.getEnlaces().getExpediente());
      ps.setString(20, pl.getEnlaces().getOpinionesPublicadas());
      ps.setString(21, pl.getEnlaces().getOpinionesPublicar());
      ps.addBatch();
    }
  }

  static class IniciativaAgrupadaTableLoad extends TableLoad {

    public IniciativaAgrupadaTableLoad() {
      super("iniciativa_agrupada");
    }

    @Override String createTableStatement() {
      return """
          create table %s (
            numero_unico string,
            iniciativa string
          )
          """.formatted(tableName);
    }

    @Override String prepareStatement() {
      return """
          insert into %s values (
            ?, ?
          )
          """.formatted(tableName);
    }

    @Override void addBatch(PreparedStatement ps, ProyectoLey pl) throws SQLException {
      for (String ia : pl.getIniciativasAgrupadas()) {
        ps.setString(1, pl.getNumeroUnico());
        ps.setString(2, ia);
        ps.addBatch();
      }
    }
  }

  static class AutorTableLoad extends TableLoad {

    public AutorTableLoad() {
      super("autor");
    }

    @Override String createTableStatement() {
      return """
          create table %s (
            numero_unico string,
            congresista string
          )
          """.formatted(tableName);
    }

    @Override String prepareStatement() {
      return """
          insert into %s values (
            ?, ?
          )
          """.formatted(tableName);
    }

    @Override void addBatch(PreparedStatement ps, ProyectoLey pl) throws SQLException {
      for (var c : pl.getAutores()) {
        ps.setString(1, pl.getNumeroUnico());
        ps.setString(2, c.getNombreCompleto());
        ps.addBatch();
      }
    }
  }

  static class AdherenteTableLoad extends TableLoad {

    public AdherenteTableLoad() {
      super("adherente");
    }

    @Override String createTableStatement() {
      return """
          create table %s (
            numero_unico string,
            congresista string
          )
          """.formatted(tableName);
    }

    @Override String prepareStatement() {
      return """
          insert into %s values (
            ?, ?
          )
          """.formatted(tableName);
    }

    @Override void addBatch(PreparedStatement ps, ProyectoLey pl) throws SQLException {
      for (var c : pl.getAdherentes()) {
        ps.setString(1, pl.getNumeroUnico());
        ps.setString(2, c);
        ps.addBatch();
      }
    }
  }

  static class CongresistaTableLoad extends TableLoad {

    public CongresistaTableLoad() {
      super("congresista");
    }

    @Override String createTableStatement() {
      return """
          create table %s (
            congresista string primary key,
            email string unique
          )
          """.formatted(tableName);
    }

    @Override String prepareStatement() {
      return """
          insert or ignore into %s values (
            ?, ?
          )
          """.formatted(tableName);
    }

    @Override void addBatch(PreparedStatement ps, ProyectoLey pl) throws SQLException {
      for (var c : pl.getAutores()) {
        ps.setString(1, c.getNombreCompleto());
        ps.setString(2, c.getCorreoElectronico());
        ps.addBatch();
      }
    }
  }

  static class SectoresTableLoad extends TableLoad {

    public SectoresTableLoad() {
      super("sector");
    }

    @Override String createTableStatement() {
      return """
          create table %s (
            numero_unico string,
            sector string
          )
          """.formatted(tableName);
    }

    @Override String prepareStatement() {
      return """
          insert into %s values (
            ?, ?
          )
          """.formatted(tableName);
    }

    @Override void addBatch(PreparedStatement ps, ProyectoLey pl) throws SQLException {
      for (var s : pl.getSectores()) {
        ps.setString(1, pl.getNumeroUnico());
        ps.setString(2, s);
        ps.addBatch();
      }
    }
  }

  static class SeguimientoTableLoad extends TableLoad {

    public SeguimientoTableLoad() {
      super("seguimiento");
    }

    @Override String createTableStatement() {
      return """
          create table %s (
            numero_unico string,
            fecha long,
            evento string
          )
          """.formatted(tableName);
    }

    @Override String prepareStatement() {
      return """
          insert into %s values (
            ?, ?, ?
          )
          """.formatted(tableName);
    }

    @Override void addBatch(PreparedStatement ps, ProyectoLey pl) throws SQLException {
      for (var s : pl.getSeguimiento()) {
        ps.setString(1, pl.getNumeroUnico());
        ps.setLong(2, s.getFecha());
        ps.setString(3, s.getEvento());
        ps.addBatch();
      }
    }
  }

  static class ExpedienteTableLoad extends TableLoad {

    public ExpedienteTableLoad() {
      super("expediente");
    }

    @Override String createTableStatement() {
      return """
          create table %s (
            numero_unico string,
            documento_tipo string,
            documento_fecha long,
            documento_titulo string,
            documento_enlace string
          )
          """.formatted(tableName);
    }

    @Override String prepareStatement() {
      return """
          insert into %s values (
            ?, ?, ?, ?, ?
          )
          """.formatted(tableName);
    }

    @Override void addBatch(PreparedStatement ps, ProyectoLey pl) throws SQLException {
      if (pl.getExpediente() != null) {
        for (var d : pl.getExpediente().getDocumentos()) {
          ps.setString(1, pl.getNumeroUnico());
          ps.setString(2, d.getTipo());
          ps.setLong(3, d.getFecha());
          ps.setString(4, d.getTitulo());
          ps.setString(5, d.getEnlace());
          ps.addBatch();
        }
      }
    }
  }
}
