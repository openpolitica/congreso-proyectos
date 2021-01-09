package openpolitica.congreso;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;

public class ProyectosLey2011 {
  public static void main(String[] args) throws IOException, SQLException {
    var app = new ProyectosLeyExtract(
        "http://www2.congreso.gob.pe",
        "/Sicr/TraDocEstProc/CLProLey2011.nsf/Local%20Por%20Numero?OpenView=&Start=",
        "/Sicr/TraDocEstProc/Expvirt_2011.nsf/visbusqptramdoc1621/%s?opendocument",
        1000);
    var proyectos = app.run();
    var avro = Path.of("data/proyectos-ley-2011.avro");
    var changed = app.save(avro, proyectos);
    if (changed) {
      var loader = new ProyectosLeyLoadSqlite();
      loader.save(avro, Path.of("data/proyectos-ley-2011.db"));
    }
  }
}
