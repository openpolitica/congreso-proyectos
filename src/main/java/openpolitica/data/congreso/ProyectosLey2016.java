package openpolitica.data.congreso;

import java.io.IOException;
import java.nio.file.Path;

public class ProyectosLey2016 {
  public static void main(String[] args) throws IOException {
    var app = new ProyectoLeyExtract(
        "http://www2.congreso.gob.pe",
        "/Sicr/TraDocEstProc/CLProLey2016.nsf/Local%20Por%20Numero?OpenView=&Start=",
        "/Sicr/TraDocEstProc/Expvirt_2011.nsf/visbusqptramdoc1621/%s?opendocument",
        100);
    var proyectos = app.run();
    app.save(Path.of("data/proyectos-ley-2016.avro"), proyectos);
  }
}
