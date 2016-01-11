int main() {

    //  Variable Declarations holding input/output filenames  

    const char * input0 = "input.txt";    
    const char * output0 = "tfidf-kmeans.txt";    
    
    //  Variable Declarations ## 

    const int numclusters0 = 4;
    const int maxiters0 = 5;
    const int numclusters1 = 4;
    
    //  Calls to Operator functions  

    var0 = readDir(input0);
    var1 = tfidf(var0, numclusters0, maxiters0);
    var2 = kmeans(var1, numclusters1);
    var3 = output(var2, output0);

}