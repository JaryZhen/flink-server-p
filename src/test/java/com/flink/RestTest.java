/**
 * Created by jaryzhen on 6/12/16.
 */


import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by jaryzhen on 6/1/16.
 */
public class RestTest {
    public static void main(String[] S) {


        get();
    }

    public static  void get() {

        try {

          //  String url = "http://10.128.32.111:8080/log/sev?page=0&size=150&search=DELETE and timestamp=2016-06-21T04:06:30.653Z_2016-06-21T04:11:30.654Z&logsize=100000&uid=all&transaction=start_SendThirdPartWorker_done:20&contains=null";
            String url = "http://10.128.32.111:8080/log/sev?page=0&size=150&search=gl2_remote_port:41670_and_source:127.0.0.1__and_timestamp=2016-06-21T07:50:50.592Z_2016-06-21T07:55:50.593Z&logsize=100000";
            URL restServiceURL = new URL(url);

            HttpURLConnection httpConnection = (HttpURLConnection) restServiceURL.openConnection();
            httpConnection.setRequestMethod("GET");
            httpConnection.setRequestProperty("Accept", "application/json");

            if (httpConnection.getResponseCode() != 200) {
                throw new RuntimeException("HTTP GET Request Failed with Error code : "
                        + httpConnection.getResponseCode());
            }

            BufferedReader responseBuffer = new BufferedReader(new InputStreamReader(
                    (httpConnection.getInputStream())));

            String output;
            System.out.println("Output from Server:  \n");

            while ((output = responseBuffer.readLine()) != null) {
                System.out.println(output);
            }

            httpConnection.disconnect();

        } catch (MalformedURLException e) {

            e.printStackTrace();

        } catch (IOException e) {

            e.printStackTrace();

        }

    }


    public static void put() {

        try {
            URL targetUrl = new URL("http://localhost:8080/log/cmd?id=2&from=0&size=1000");
            //URL targetUrl = new URL(targetURL);

            HttpURLConnection httpConnection = (HttpURLConnection) targetUrl.openConnection();
            httpConnection.setDoOutput(true);
            httpConnection.setRequestMethod("POST");
            httpConnection.setRequestProperty("Content-Type", "application/json");

            String input = "{\"id\":1,\"firstName\":\"Liam\",\"age\":22,\"lastName\":\"Marco\"}";

            OutputStream outputStream = httpConnection.getOutputStream();
            outputStream.write(input.getBytes());
            outputStream.flush();

            if (httpConnection.getResponseCode() != 200) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + httpConnection.getResponseCode());
            }

            BufferedReader responseBuffer = new BufferedReader(new InputStreamReader(
                    (httpConnection.getInputStream())));

            String output;
            System.out.println("Output from Server:\n");
            while ((output = responseBuffer.readLine()) != null) {
                System.out.println(output);
            }

            httpConnection.disconnect();

        } catch (MalformedURLException e) {

            e.printStackTrace();

        } catch (IOException e) {

            e.printStackTrace();

        }

    }

}