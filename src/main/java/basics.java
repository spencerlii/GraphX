
class cal{

    int i;
    int j;
    int k;
    static String ceo;

    public cal(){
    i =5;
    j=10;
    }

    public cal(int n){
        k=n;
    }

    int res;


    public void per(){
        res = i+j;
    }
}





class BasicJava {

    // we take String[] args as string can take any of float, int ... and an array as parameters can be more than one
    // static so that we can call the function/variable... without defining the object, it will remain same throughout the class like 
    //variable company ceo it can be called without object
    // same for main method, we don't need object to call the main function so static 
    public static void main(String[] args) {
        


        // data type name[] = values, but here emplty array of type 4
        // int num[] = new int[4];
        int num[] = {1,2,3,4};
        num[0] = 5;
        System.out.println(num[0]);

        int temp[][] = {
            {1,2,4,5},
            {2,35,1,3}
        };

        BasicJava.Excepdemo();

        // Basic class
        // new to allocate memory
        // cal.ceo = "Himanshu";
        // cal obj = new cal();
        // System.out.println(obj.i);
        // obj.i = 5;
        // obj.j = 10;
        // obj.per();
        // System.out.println(obj.res);

        // basic operators
        // BasicJava.oper();
        
        // BasicJava.sample();
        // BasicJava.java_loop();

    }

    public static void Excepdemo(){
        try{
            int s = 9/0 ;
        }
        catch(Exception e){
            System.out.println("error ");
        }
    }




    public static void oper(){
        // implecit conversion -> it will be cnoverted to int
        double s = 5;

        // explecit conversion
        int k = (int)5.5;


        // it will print B
        char c = 66;
        
        System.out.println(k);
    }




// Basic conditional statements
    public static void java_cond() {
        int n= 7;
        if (n==7)
        {
            System.out.println("it is seven");
        }
        else
        {
            System.out.println("not seven");
        }

        System.out.println("hello world"); 
    }


// basic looping statements
    public static void java_loop() {
        
        for(int i=0;i<5;i++){

            System.out.println("hello world"+" "+(i+1));  
        } 
    }








}