import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;

public class Main
{
    private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    private static String rootPath = "hdfs://45caaf5e50a3:8020";

    private static String fileTxt = "/test/file1.txt";

    public static void main(String[] args) throws Exception
    {
        FileAccess fileAccess = new FileAccess(rootPath);

        String filePath = rootPath + fileTxt;
        fileAccess.delete(filePath);
        fileAccess.create(filePath);

        StringBuilder stringBuilder = new StringBuilder();
        for(int i = 0; i < 10_000; i++) {
            stringBuilder.append(getRandomWord() + " ");
        }

        fileAccess.append(filePath, stringBuilder.toString());

        System.out.println("FILE...................");
        System.out.println(fileAccess.read(filePath));
        System.out.println("PATH...................");
        fileAccess.list(rootPath + "/test/").stream().forEach(System.out::println);

        fileAccess.close();
    }

    private static String getRandomWord()
    {
        StringBuilder builder = new StringBuilder();
        int length = 2 + (int) Math.round(10 * Math.random());
        int symbolsCount = symbols.length();
        for(int i = 0; i < length; i++) {
            builder.append(symbols.charAt((int) (symbolsCount * Math.random())));
        }
        return builder.toString();
    }
}
