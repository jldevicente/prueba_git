package org.sourygna;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AnalisisLogsDriver {
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: AnalisisLogs <input dir> <output dir>\n");
			System.exit(-1);
		}
		// Añadimos la entrada de argumentos.
		String input = args[0];
		String output = args[1];
		// Instanciamos el Job
		Job job = Job.getInstance();
		job.setJarByClass(AnalisisLogsDriver.class);
		job.setJobName("AnalisisLogs");

		// Añadimos la ruta de entrada
		FileInputFormat.setInputPaths(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		// Añadimos el Mapper y los valores de salida del Key,Values.
		job.setMapperClass(MapperAnalisis.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Añadimos el Reduccer y los valores de salida del Key y Values.
		job.setReducerClass(ReducerAnalisis.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Se define el tipo y ruta de salida.
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(output));

		// Lanzamos el Job.
		boolean success = job.waitForCompletion(true);

		// Recuperamos los valores de los contadores de mi grupo creado.
		CounterGroup group = job.getCounters().getGroup("aplicaciones");
		// Escribimos por pantalla los valores del grupo en este caso el numero
		// de veces que se repite
		// una aplicacion.
		for (Counter g : group) {
			System.out.println(g.getName() + "-->" + g.getValue());
		}

		System.exit(success ? 0 : 1);
	}

}
