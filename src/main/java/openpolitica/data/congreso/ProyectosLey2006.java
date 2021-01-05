package openpolitica.data.congreso;

import java.io.IOException;
import java.nio.file.Path;

public class ProyectosLey2006 {
  public static void main(String[] args) throws IOException {
    var app = new ProyectosLeyExtract(
        "http://www2.congreso.gob.pe",
        "/Sicr/TraDocEstProc/CLProLey2006.nsf/Local%20Por%20Numero?OpenView=&Start=",
        "/sicr/tradocestproc/TraDoc_expdig_2006.nsf/5C26E09BB2A7CFDA052574AC005DA5B7/%s?opendocument",
        500);
    var proyectos = app.run();
    app.save(Path.of("data/proyectos-ley-2006.avro"), proyectos);
  }
}
