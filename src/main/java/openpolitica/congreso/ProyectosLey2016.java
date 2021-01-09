package openpolitica.congreso;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;

public class ProyectosLey2016 {
  public static void main(String[] args) throws IOException, SQLException {
    var ext = new ProyectosLeyExtract(
        "http://www2.congreso.gob.pe",
        "/Sicr/TraDocEstProc/CLProLey2016.nsf/Local%20Por%20Numero?OpenView=&Start=",
        "/Sicr/TraDocEstProc/Expvirt_2011.nsf/visbusqptramdoc1621/%s?opendocument",
        100);
    var proyectos = ext.run();
    var avro = Path.of("data/proyectos-ley-2016.avro");
    var changed = ext.save(avro, proyectos);
    if (changed) {
      var loader = new ProyectosLeyLoadSqlite();
      loader.save(avro, Path.of("data/proyectos-ley-2016.db"));
    }
  }
}
