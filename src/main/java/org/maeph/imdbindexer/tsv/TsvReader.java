package org.maeph.imdbindexer.tsv;

import com.google.gson.Gson;
import io.reactivex.Observable;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.maeph.imdbindexer.model.TitleBasicInfo;
import org.maeph.imdbindexer.model.TitlePrincipal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.*;


import java.util.zip.GZIPInputStream;

public class TsvReader {

    public static final String NULL_MARKER = "\\N";
    public static final String BASIC_TITLE_INFO_TYPE = "basic_title_info";
    public static final String PRINCIPAL_CAST_TYPE = "principal_cast";
    private static Map<String, String> movies = new HashMap<>();
    private static Map<String, String> names = new HashMap<>();


    public static void main(String[] args) throws IOException {

        System.out.println("Preparing name cache...");
        cacheNames();
        System.out.println("Name cache ready.");

        System.out.println("Indexing titles basic info...");
        indexFile("title.basics.tsv.gz", TsvReader::convertBasic, BASIC_TITLE_INFO_TYPE);
        System.out.println("Titles basic info indexed, index: " + BASIC_TITLE_INFO_TYPE + "/" + BASIC_TITLE_INFO_TYPE);

        System.out.println("Indexing principal cast...");
        indexFile("title.principals.tsv.gz", TsvReader::convertPrincipal, PRINCIPAL_CAST_TYPE);
        System.out.println("Principal cast indexed, index: " + PRINCIPAL_CAST_TYPE + "/" + PRINCIPAL_CAST_TYPE);
    }

    private static void cacheNames() throws IOException {
        BufferedReader reader = fromGzReader("name.basics.tsv.gz");

        reader.lines()
                .forEach(line -> {
                    StringTokenizer tokenizer = tokenizer(line);
                    names.put(tokenizer.nextToken(), tokenizer.nextToken());
                });
    }

    private static void cacheTitles() throws IOException {
        BufferedReader reader = fromGzReader("title.basics.tsv.gz");

        reader.lines()
                .forEach(line -> {
                    StringTokenizer tokenizer = tokenizer(line);
                    String key = tokenizer.nextToken();
                    tokenizer.nextToken();
                    movies.put(key, tokenizer.nextToken());
                });
    }

    private static BufferedReader fromGzReader(String name) throws IOException {
        InputStream basicsInputStream = TsvReader.class.getClassLoader().getResourceAsStream(name);

        GZIPInputStream gzip = new GZIPInputStream(basicsInputStream);
        return new BufferedReader(new InputStreamReader(gzip, Charset.defaultCharset().toString()));
    }


    private static void indexFile(String fileName, Function<String, ?> readBasic, String type) throws IOException {
        BufferedReader reader = fromGzReader(fileName);
        Gson gson = new Gson();


        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        );

        Observable.fromIterable(() -> reader.lines().iterator())
                .skip(1)
                .map(readBasic)
                .map(gson::toJson)
                .map(source -> new IndexRequest(type, type).source(source, XContentType.JSON))
                .buffer(10000)
                .subscribe(requestBatch -> {
                    BulkRequest bulkRequest = new BulkRequest();
                    requestBatch.forEach(bulkRequest::add);
                    client.bulk(bulkRequest);
                });

        client.close();
    }

    private static TitleBasicInfo convertBasic(String line) {
        StringTokenizer tokenizer = tokenizer(line);

        TitleBasicInfo basicInfo = TitleBasicInfo.builder()
                .tconst(tokenizer.nextToken())
                .titleType(tokenizer.nextToken())
                .primaryTitle(tokenizer.nextToken())
                .originalTitle(tokenizer.nextToken())
                .isAdult(!tokenizer.nextToken().equals("0"))
                .startYear(getOptionalIntValue((tokenizer)))
                .endYear(getOptionalIntValue(tokenizer))
                .runtimeMinutes(getOptionalIntValue(tokenizer))
                .genres(getMovieGenres(tokenizer))
                .build();

        movies.put(basicInfo.getTconst(), basicInfo.getPrimaryTitle());

        return basicInfo;
    }

    private static TitlePrincipal convertPrincipal(String line) {
        StringTokenizer tokenizer = tokenizer(line);

        return TitlePrincipal.builder()
                .movieTitle(movies.get(tokenizer.nextToken()))
                .ordering(Integer.valueOf(tokenizer.nextToken()))
                .name(names.get(tokenizer.nextToken()))
                .category(tokenizer.nextToken())
                .job(getOptionalStringValue(tokenizer))
                .characters(getCharacters(tokenizer))
                .build();
    }

    private static StringTokenizer tokenizer(String line) {
        return new StringTokenizer(line, "\t");
    }



    private static String[] getMovieGenres(StringTokenizer tokenizer) {
        String s = tokenizer.nextToken();
        return s.equals(NULL_MARKER) ? null : s.split(",");
    }

    private static String[] getCharacters(StringTokenizer tokenizer) {
        String s = tokenizer.nextToken();
        return s.equals(NULL_MARKER) ? null :
            s.replace("[", "").replace("]", "").split(",");

    }


    private static Integer getOptionalIntValue(StringTokenizer tokenizer) {
        String value = tokenizer.nextToken();
        return value.equals(NULL_MARKER) ? null : Integer.valueOf(value);
    }

    private static String getOptionalStringValue(StringTokenizer tokenizer) {
        String value = tokenizer.nextToken();
        return value.equals(NULL_MARKER) ? null : value;
    }

}
