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
        NgramBuilder.buildNgram(args[0],"output\\NgramLibrary");

        //build unigram
        ExtractCKN.extract("output\\NgramLibrary","output\\1gramWords",1);
        Probability.calcProb("output\\1gramWords","output\\1gramProbs",0);

        //iteration,low order
        for(int i=2;i<noGram;++i){
            ExtractCKN.extract("output\\NgramLibrary","output\\"+i+"gramWords",i);
            CalculateDriver.calculateProb("output\\"+i+"gramWords","output\\"+i+"gramProbs","output\\"+(i-1)+"gramProbs",discount);
        }
        //highest order
        ExtractCount.extract("output\\NgramLibrary","output\\"+noGram+"gramWords",noGram);
        CalculateDriver.calculateProb("output\\"+noGram+"gramWords","output\\"+output,"output\\"+(noGram-1)+"gramProbs",discount);

    }
}
