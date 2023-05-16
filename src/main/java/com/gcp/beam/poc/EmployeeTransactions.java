package com.gcp.beam.poc;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class EmployeeTransactions {

	public static void main(String[] args) throws Exception {
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);
		PCollection<String> transData = p.apply(TextIO.read().from("C:\\Users\\002BST744\\Documents\\input\\Transactions.csv"));
		// Convert String of each lines from csv to pair of key and value
		PCollection<KV<String, Integer>> pOutput = transData.apply(ParDo.of( new StringToKV()));
		// apply Group by
        PCollection<KV<String,Iterable<Integer>>> kvpCollection = pOutput.apply(GroupByKey.<String,Integer>create());
        PCollection<String> output = kvpCollection.apply(ParDo.of(new KVToString()));
        output.apply(TextIO.write().to("C:\\Users\\002BST744\\Documents\\Output\\GroupByKey_out_data.csv").withHeader("empId,TransactionType,amount").withNumShards(1).withSuffix(".csv"));
        p.run();
		
	}
	

}
