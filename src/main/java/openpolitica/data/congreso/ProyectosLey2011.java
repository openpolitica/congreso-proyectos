package openpolitica.data.congreso;

import java.io.IOException;
import java.nio.file.Path;

public class ProyectosLey2011 {
  public static void main(String[] args) throws IOException {
    var app = new ProyectosLeyExtract(
        "http://www2.congreso.gob.pe",
        "/Sicr/TraDocEstProc/CLProLey2011.nsf/Local%20Por%20Numero?OpenView=&Start=",
        "/Sicr/TraDocEstProc/Expvirt_2011.nsf/visbusqptramdoc1621/%s?opendocument",
        1000);
    var proyectos = app.run();
    app.save(Path.of("data/proyectos-ley-2011.avro"), proyectos);
  }
}
