package NgramWithSmoothing;

import NgramWithSmoothing.Calculation.*;
import NgramWithSmoothing.ExtractKgram.*;
import org.apache.log4j.BasicConfigurator;


public class Driver {

    public static void main(String[] args) throws Exception {
        //args: input path,output path,ngram,d

        //log4j configure
        BasicConfigurator.configure();

        //trim "\"
        String output = args[1];
        if(output.charAt(output.length()-1)=='/' || output.charAt(output.length()-1)=='\\'){
            output = output.substring(0,output.length()-1);
        }
        //parse other args
        int noGram = Integer.parseInt(args[2]);
        double discount = Double.parseDouble(args[3]);

        //build ngram library
        NgramBuilder.buildNgram(args[0],"NgramLibrary");

        //build unigram
        ExtractCKN.extract("NgramLibrary","1gramWords",1);
        Probability.calcProb("1gramWords","1gramProbs",discount);

        //iteration,low order
        for(int i=2;i<noGram;++i){
            ExtractCKN.extract("NgramLibrary",i+"gramWords",i);
            Summation.calculate(i+"gramWords",i+"gramProbs",i-1+"gramProbs",discount);
        }
        //highest order
        ExtractCount.extract("NgramLibrary",noGram+"gramWords",noGram);
        Summation.calculate(noGram+"gramWords",output,noGram-1+"gramProbs",discount);
    }
}
