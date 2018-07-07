package NgramWithSmoothing;

import NgramWithSmoothing.Calculator.Interpolator;
import NgramWithSmoothing.Calculator.LambdaCalculator;
import NgramWithSmoothing.Calculator.ProbabilityCalculator;
import NgramWithSmoothing.DBWriter.KVWriter;
import NgramWithSmoothing.DBWriter.ProbWriter;
import NgramWithSmoothing.KgramExtractor.CKNExtractor;
import NgramWithSmoothing.KgramExtractor.CountExtractor;
import NgramWithSmoothing.KgramExtractor.NgramBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Driver extends Configured implements Tool{

    @Override
    public int run(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        Configuration conf = getConf();
        //args: input path,output path,ngram,threshold,d
        //trim "\"
        String output = args[1];
        if(output.charAt(output.length()-1)!='/' && output.charAt(output.length()-1)!='\\'){
            output+="/";
        }
        String grams = output+"grams/";
        String pivot = output+"pivot/";
        String model = output+"model/";
        //parse other args
        int noGram = Integer.parseInt(args[2]);
        double discount = Double.parseDouble(args[4]);

        //build ngram library
        System.out.println("starting building library");
        NgramBuilder.buildNgram(args[0],grams+"library",noGram,Integer.parseInt(args[3]));
        //build unigram
        System.out.println("starting calculate unigram");
        CKNExtractor.extract(grams+"library",grams+"1",1);
        ProbabilityCalculator.calcProb(grams+"1",model+"prob1",0);
        //iteration,low order
        for(int i=2;i<noGram;++i){
            System.out.println("starting calculate "+i+" gram");
            CKNExtractor.extract(grams+"library",grams+i,i);
            ProbabilityCalculator.calcProb(grams+i,pivot+"prob"+i,discount);
            LambdaCalculator.calcLambda(grams+i,model+"lambda"+i,discount);
        }
        //highest order
        System.out.println("starting calculate highest gram");
        CountExtractor.extract(grams+"library",grams+noGram,noGram);
        ProbabilityCalculator.calcProb(grams+noGram,pivot+"prob"+noGram,discount);
        LambdaCalculator.calcLambda(grams+noGram,model+"lambda"+noGram,discount);
        //Interpolate
        for (int i=2;i<noGram+1;++i){
            System.out.println("interpolating "+i);
            Interpolator.interpolate(
                    model+"prob"+(i-1),
                    pivot+"prob"+i,
                    model+"lambda"+i,
                    model+"prob"+i,
                    i,
                    i-1
            );
        }
        //write to hbase
        System.out.println("write to db"+1);
        ProbWriter.write(model+"prob"+1,"order1");
        for (int i=2;i<noGram+1;++i){
            System.out.println("write to db"+i+"predict");
            ProbWriter.write(model+"prob"+i,"order"+i);
            System.out.println("write to db"+i+"lambda");
            KVWriter.write(model+"lambda"+i,"order"+i);
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("executing main");
        BasicConfigurator.configure();
        int exitCode = ToolRunner.run(new Driver(), args);
        System.exit(exitCode);
    }
}
