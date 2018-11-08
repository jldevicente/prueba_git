package org.sourygna;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/*Usamos un In mapper combiner nos permite realizar las operaciones en el mapper y evitar el flujo de informacion inecesaria por la red.
 * La salida del maper seria  Key DD/MM/AAAA-HH Values (APP:nº repeticiones,APP:nº Repeticiones......) 
*/
public class MapperAnalisis extends Mapper<LongWritable, Text, Text, Text> {
	// Definimos variables.
	private Map<String, Integer> buffer;
	private Text outputKey = new Text();
	private Text outputValue = new Text();
    

	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		//Definimos un hasmap usado de almacenamiento temporal.
		buffer = new HashMap<String, Integer>();
	}

	@Override
	protected void cleanup(
			
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		//Recorremos la Hasmap:
		for (Entry<String, Integer> entry : buffer.entrySet()) {
            //Separamos la clave compuesta.
			String[] parteClave = entry.getKey().split("\t");
			//Añadimos el valor a la variable KEY
			outputKey.set(parteClave[0]);
			//Añadimos el valor a la variable de Values, compuesta por (aplicacion: nº de veces que se repite)
			outputValue.set(parteClave[1] + ":" + entry.getValue().toString());
			context.write(outputKey, outputValue);
		}
	}

/*
 * En el mapper vamos a crear una Key compuesta entre la fecha y la aplicacion
 *  y el value es la suma de la veces que se va repitiendo la aplicacion.
 *  Se alamacenara enun HAshMap(buffer).
 *  
 *  A su vez definiremos los contadores.
 *  */
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
        //Definimos las variables
		String[] trozos = value.toString().split(" ");
		String mes = trozos[0];
		String dia = trozos[1];
		String tiempo = trozos[2];
		String componente = formateoComponente(trozos[4]);
		
		//Descartamos todas aquellas aplicacioes que no contengan
		// vmnet.
		if (componente.matches("(.*)vmnet(.*)")) {
			return;
		}
		//Formateamos elementos
		String fechaFormateada = formateoFecha(mes, dia,
				this.formateoTiempo(tiempo));
		String componenteFormateada = formateoComponente(componente);
		//Creamos key compuesta.
		String clave = fechaFormateada + "\t" + componenteFormateada;
		
		//Comprobamos si existe en la HasMap
		// Si NO existe añadimos la fecha app = 1
		if (!(buffer.containsKey(clave))) {
			buffer.put(clave, 1);
		} 
		//si existe sumamos 1 al numero de veces que se repite.
		else {
			Integer aux = buffer.get(clave) + 1;
			buffer.put(clave, aux);

		}
        
		//Incrementamos 1 al contador.
		context.getCounter("aplicaciones", componenteFormateada).increment(1);
		;

	}

	public String formateoFecha(String mes, String dia, String tiempo) {
		//Formateramos la fecha de salida 
		// DD/MM/AAAA-HH
		Map<String, String> meses = new HashMap<String, String>();
		meses.put("Ene", "01");
		meses.put("Feb", "02");
		meses.put("Mar", "03");
		meses.put("Abr", "04");
		meses.put("May", "05");
		meses.put("Jun", "06");
		meses.put("Jul", "07");
		meses.put("Ago", "08");
		meses.put("Sep", "09");
		meses.put("Oct", "10");
		meses.put("Nov", "11");
		meses.put("Dic", "12");

		String result = dia + "/" + meses.get(mes) + "/" + "2014" + "-"
				+ tiempo;

		return result;

	}

	public String formateoTiempo(String tiempo) {
		//Formatea la salida del tiempo.
		// HH
		String[] partesTiempo = tiempo.split(":");
		return partesTiempo[0];

	}

	public String formateoComponente(String comp) {
		//Formatea el componente
		// Quita [ y :
		String[] parteComp = comp.split("\\[");
		Pattern p = Pattern.compile("[:]");
		Matcher m = p.matcher(parteComp[0]);
		if (m.find()) {
			parteComp[0] = m.replaceAll("");
		}

		return parteComp[0];

	}

}
