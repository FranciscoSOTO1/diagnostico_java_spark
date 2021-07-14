package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import minsait.ttaa.common.pathfile.PathFile;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {
	private SparkSession spark;
	
	public Transformer(@NotNull SparkSession spark,PathFile path) {
		this.spark = spark;
		
		Dataset<Row> df = readInput(path.getInput());

		df.printSchema();

		df = cleanData(df);
		df = NationalityOverall(df);
		df = potOv(df);
		df = Filtrado_datos(df);
		df = columnSelection(df);

		df.show(100, false);
		// for show 100 records after your transformations and show the Dataset schema
		df.printSchema();
		write(df,path.getOutput());
		
	}

	private Dataset<Row> columnSelection(Dataset<Row> df) {
		return df.select(
				shortName.column(),
				longName.column(),
				age.column(),
				heightCm.column(),
				weightKg.column(),
				nationality.column(),
				clubName.column(),
				overall.column(),
				potential.column(),
				teamPosition.column(),
				playerCat.column(),
				potentialVsOverall.column()
				);
	}

	/**
	 * @return a Dataset readed from csv file
	 */
	private Dataset<Row> readInput(String input) {
		Dataset<Row> df = spark.read()
				.option(HEADER, true)
				.option(INFER_SCHEMA, true)
				.csv(input);
		return df;
	}

	/**
	 * @param df
	 * @return a Dataset with filter transformation applied
	 * column team_position != null && column short_name != null && column overall != null
	 */
	private Dataset<Row> cleanData(Dataset<Row> df) {
		df = df.filter(
				teamPosition.column().isNotNull().and(
						shortName.column().isNotNull()
						).and(
								overall.column().isNotNull()
								)
				);

		return df;
	}

	/**
	 * @param df is a Dataset with players information (must have team_position and height_cm columns)
	 * @return add to the Dataset the column "cat_height_by_position"
	 * by each position value
	 * cat A for if is in 20 players tallest
	 * cat B for if is in 50 players tallest
	 * cat C for the rest
	 */

	private Dataset<Row> NationalityOverall(Dataset<Row> df) {
		WindowSpec w = Window
				.partitionBy(nationality.column())
				.orderBy(overall.column().desc());

		Column rank = rank().over(w);

		Column rule = when(rank.$less(10), A)
				.when(rank.$less(20), B)
				.when(rank.$less(50), C)
				.otherwise(D);

		df = df.withColumn(playerCat.getName(), rule);

		return df;
	}

	
	//DIVIDIR
	private Dataset<Row> potOv(Dataset<Row> df) {

		return df.withColumn(potentialVsOverall.getName(), 
				col(potential.getName()).divide(col(overall.getName())));
	}

//problema 4 del ejercicio.

	private Dataset<Row> Filtrado_datos(Dataset<Row> df) {
		df = df.filter(col(playerCat.getName()).equalTo(A)
				.or(col(playerCat.getName()).equalTo(B))
				.or(col(playerCat.getName()).equalTo(C)
						.and(col(potentialVsOverall.getName()).$greater(1.15)))
				.or(col(playerCat.getName()).equalTo(D)
						.and(col(potentialVsOverall.getName()).$greater(1.25)))
				);

		return df;
	}


}
