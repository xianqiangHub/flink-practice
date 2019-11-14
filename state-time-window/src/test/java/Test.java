public class Test {

    public static void main(String[] args) {

        Long  timestamp = 1573390686756L;

        Long a = (timestamp + 5000L) % 5000L;

        System.out.println(a);
    }
}
