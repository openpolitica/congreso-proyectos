package openpolitica.congreso;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;

public class ProyectosLey2006 {
  public static void main(String[] args) throws IOException, SQLException {
    var app = new ProyectosLeyExtract(
        "http://www2.congreso.gob.pe",
        "/Sicr/TraDocEstProc/CLProLey2006.nsf/Local%20Por%20Numero?OpenView=&Start=",
        "/sicr/tradocestproc/TraDoc_expdig_2006.nsf/5C26E09BB2A7CFDA052574AC005DA5B7/%s?opendocument",
        500);
    var proyectos = app.run();
    var avro = Path.of("data/proyectos-ley-2006.avro");
    var changed = app.save(avro, proyectos);
    if (changed) {
      var loader = new ProyectosLeyLoadSqlite();
      loader.save(avro, Path.of("data/proyectos-ley-2006.db"));
    }
  }
}
