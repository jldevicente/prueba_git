package org.sourygna;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/*
 * El reducer recibe la informacion ya totalmente procesada solo se encarga de recorrer los valoresy juntarlos
 */

public class ReducerAnalisis extends Reducer<Text  ,Text, Text,Text> {
	
   
	Text valor = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
           String cadena="";
           //Recorremos los valores y lo juntamos en un String.
		   for (Text value : values){
			   cadena=cadena +value.toString()+",";
			   
		   }
		   valor.set(cadena);
		   context.write(key, valor);
	}
	
}
