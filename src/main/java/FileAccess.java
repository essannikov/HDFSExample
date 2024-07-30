import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FileAccess
{
    private FileSystem hdfs;

    /**
     * Initializes the class, using rootPath as "/" directory
     *
     * @param rootPath - the path to the root of HDFS,
     * for example, hdfs://localhost:32771
     */
    public FileAccess(String rootPath) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        System.setProperty("HADOOP_USER_NAME", "root");

        hdfs = FileSystem.get(
                new URI(rootPath), configuration
        );
    }

    /**
     * Creates empty file or directory
     *
     * @param path
     */
    public void create(String path) throws Exception
    {
        Path file = new Path(path);

        OutputStream os = hdfs.create(file);
    }

    /**
     * Appends content to the file
     *
     * @param path
     * @param content
     */
    public void append(String path, String content) throws Exception
    {
        Path file = new Path(path);

        String contentOld = "";

        if (hdfs.exists(file)){
            contentOld = read(path);
            hdfs.delete(file, true);
        }

        //OutputStream os = hdfs.append(file);
        OutputStream os = hdfs.create(file);

        BufferedWriter br = new BufferedWriter(
                new OutputStreamWriter(os, StandardCharsets.UTF_8)
        );

        br.write(contentOld + content);

        br.flush();
        br.close();
    }

    /**
     * Returns content of the file
     *
     * @param path
     * @return
     */
    public String read(String path) throws Exception
    {
        Path file = new Path(path);

        InputStream is = hdfs.open(file);

        BufferedReader br = new BufferedReader(
                new InputStreamReader(is, StandardCharsets.UTF_8)
        );
        String result = br.lines().collect(Collectors.joining("\n"));

        br.close();

        return result;
    }

    /**
     * Deletes file or directory
     *
     * @param path
     */
    public void delete(String path) throws Exception
    {
        Path file = new Path(path);

        if (hdfs.exists(file)) {
            hdfs.delete(file, true);
        }
    }

    /**
     * Checks, is the "path" is directory or file
     *
     * @param path
     * @return
     */
    public boolean isDirectory(String path) throws Exception
    {
        Path file = new Path(path);

        return hdfs.isDirectory(file);
    }

    /**
     * Return the list of files and subdirectories on any directory
     *
     * @param path
     * @return
     */
    public List<String> list(String path) throws Exception
    {
        Path file = new Path(path);

        List<String> resultList = new ArrayList<>();

        RemoteIterator<LocatedFileStatus> fileIterator = hdfs.listFiles(file, false);
        while (fileIterator.hasNext()){
            LocatedFileStatus fileStatus = fileIterator.next();
            resultList.add(fileStatus.getPath().toString());
        }

        return resultList;
    }

    public void close() throws Exception{
        hdfs.close();
    }
}
