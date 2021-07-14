package minsait.ttaa.common.pathfile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.stream.Stream;
import static minsait.ttaa.datio.common.Common.CONFIG_PATH;

public class PathFile {


	private  String input;
	private  String output;


	/*
	 * Se lee la ruta de los archivos para la lectura y escritura 
	 * la primer linea corresponde a la ruta del archivo de lectura 
	 * la segunda linea corresponde a la ruta de escritura
	 */
	public  PathFile() {

		Path path = Paths.get(CONFIG_PATH);

		try (Stream<String> stream = Files.lines(path)) {

			Iterator<String> p=stream.iterator();
			this.input=p.next().toString();
			this.output=p.next().toString();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public String getInput() {
		return input;
	}

	public String getOutput() {
		return output;
	}


}
