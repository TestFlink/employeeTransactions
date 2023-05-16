package com.gcp.beam.poc;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class KVToString extends DoFn<KV<String,Iterable<Integer>>,String>{
    @ProcessElement
    public void processElement(ProcessContext c){
        String strKey = c.element().getKey();
        String[] data=strKey.split("-");
        Iterable<Integer> values = c.element().getValue();
        Integer sum =0;
        for (Integer integer:values){
            sum+=integer;
        }
        c.output(data[0]+","+data[2]+","+sum.toString());
    }
}