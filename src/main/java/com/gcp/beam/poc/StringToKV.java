package com.gcp.beam.poc;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
public 	class StringToKV extends DoFn<String,KV<String,Integer>>{
    @ProcessElement
    public void processElement(ProcessContext c){
        String input = c.element();
        String arr[] = input.split(",");
        
        c.output(KV.of(arr[0]+"-"+arr[1]+"-"+arr[2],Integer.valueOf(arr[3])));
    }
}
