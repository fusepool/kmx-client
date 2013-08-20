/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.treparel.kmxclient;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniel van Adrichem
 */
public class KMXClient {

    private String sessionKey;
    private String baseURL = "http://192.168.1.87:9090/kmx/api/v1/";
    private String authStringEnc;
    private static final Logger log = LoggerFactory.getLogger(KMXClient.class);

    public KMXClient() {
        // TODO: fix use the create session rest endpoint on kmx
        this.sessionKey = "32478fb9ac51d46919bcbe02cd229c42a004e939";

        String username = "test";
        String password = "test";
        String authString = username + ":" + password;
        byte[] authEncBytes = Base64.encodeBase64(authString.getBytes());
        this.authStringEnc = new String(authEncBytes);
    }

    private HttpURLConnection doRequest(String url, String body, String method) throws RuntimeException, IOException {
        URL requestUrl;
        String requestString = url;
        try {
            requestUrl = new URL(requestString.toString());
            System.out.println(" > kmx request: " + requestUrl);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Unable to build valid request URL for " + requestString);
        }

        HttpURLConnection connection = (HttpURLConnection) requestUrl.openConnection();
        connection.setUseCaches(false);
        connection.setDoInput(true);
        connection.setDoOutput(true);
        connection.setRequestMethod(method);
        connection.setRequestProperty("Authorization", "Basic " + authStringEnc);
        if (body.length() > 0) {
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connection.setRequestProperty("Content-Length", ""
                    + Integer.toString(body.getBytes().length));
        }
        DataOutputStream wr = new DataOutputStream(
                connection.getOutputStream());
        wr.writeBytes(body);
        wr.flush();
        wr.close();

        return connection;
    }

    private JSONObject doPost(String url, String data) throws RuntimeException, IOException {
        return handleResponse(doRequest(url, data, "POST"));
    }

    private JSONObject doPut(String url, String data) throws RuntimeException, IOException {
        return handleResponse(doRequest(url, data, "PUT"));
    }

    private JSONObject doGet(String url) throws RuntimeException, IOException {
        return handleResponse(doRequest(url, "", "GET"));
    }

    private JSONObject handleResponse(HttpURLConnection connection) throws IOException {
        if (connection.getResponseCode() != 200) {
            throw new IOException("Error " + connection.getResponseCode());
        }

        String result = IOUtils.toString(connection.getInputStream());
        try {
            JSONObject root = new JSONObject(result);
            if (root.has("error_message")) {
                String msg = root.getString("error_message");
                if (root.has("detail_message")) {
                    msg = root.getString("detail_message");
                }
                throw new IOException("KMX responded with:\n" + msg);
            }
            // we're good
            return root;
        } catch (JSONException e) {
            log.error("Unable to parse Response for Request " + connection.getURL());
            log.error("ResponseData: \n" + result);
            throw new IOException("Unable to parse JSON from Results for Request " + connection.getURL(), e);
        }
    }

    public JSONObject listDatasets() throws IOException {
        return listDatasets(0);
    }

    public JSONObject listDatasets(int parent) throws IOException {
        String url = this.baseURL + "data/list?session=" + this.sessionKey;
        url += "&parent=" + parent;
        return doGet(url);
    }

    public JSONObject createDataset(int parent) throws IOException {
        String url = this.baseURL + "data/create?session=" + this.sessionKey;
        return doPost(url, "ignored");
    }

    public JSONObject addItemToDataset(int datasetId, Map item) throws IOException {
        String url = this.baseURL + "data/" + datasetId + "/add?session=" + this.sessionKey;
        String body;
        try {
            JSONObject data = new JSONObject();
            Iterator it = item.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pairs = (Map.Entry) it.next();
                data.put((String) pairs.getKey(), (String) pairs.getValue());
            }
            body = data.toString();
        } catch (JSONException ex) {
            throw new IOException("Unable to serialize text", ex);
        }
        return doPost(url, body);
    }

    // TODO: add_items_to_dataset(self, dataset_id, items):
    // TODO: list_items_in_dataset(self, dataset_id):
    // TODO: get_dataset_item_contents(self, dataset_id, item_id):
    // TODO: update_dataset_item_contents(self, dataset_id, item_id, item):
    // TODO: get_dataset_info(self, dataset_id):
    // TODO: get_all_datasets_info(self):
    
    public JSONObject createWorkspace() throws IOException {
        String url = this.baseURL + "workspace/create?session=" + this.sessionKey;
        return doPost(url, "ignored");
    }

    public JSONObject addDatasetToWorkspace(int workspaceId, int datasetId,
            HashMap features) throws IOException {
        String url = this.baseURL + "workspace/%s/add?session=" + this.sessionKey;
        String body;
        try {
            JSONObject data = new JSONObject();
            data.put("source dataset_id", datasetId);
            data.put("features", features);
            body = data.toString();
        } catch (JSONException ex) {
            throw new IOException("Unable to serialize text", ex);
        }
        return doPost(url, body);
    }

    public JSONObject labelWorkspaceItems(int workspaceId, Map caseLabels) throws IOException {
        String url = this.baseURL + "workspace/" + workspaceId + "/label?session=" + this.sessionKey;
        String body;
        try {
            JSONObject data = new JSONObject();
            Iterator it = caseLabels.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pairs = (Map.Entry) it.next();
                data.put((String) pairs.getKey(), (String) pairs.getValue());
            }
            body = data.toString();
        } catch (JSONException ex) {
            throw new IOException("Unable to serialize text", ex);
        }
        return doPost(url, body);
    }

    public JSONObject createStoplist() throws IOException {
        String url = this.baseURL + "stoplist/create?session=" + this.sessionKey;
        return doPost(url, "ignored");
    }

    public JSONObject AddWordsToStoplist(String stoplistName, List<String> words) throws IOException {
        String url = this.baseURL + "stoplist/" + stoplistName + "/add?session=" + this.sessionKey;
        String body;
        try {
            JSONObject data = new JSONObject();
            data.put("words", words);
            body = data.toString();
        } catch (JSONException ex) {
            throw new IOException("Unable to serialize text", ex);
        }
        return doPost(url, body);
    }

    public JSONObject createSVMModel(int workspaceId, Map settings) throws IOException {
        String url = this.baseURL + "model/svm/create?session=" + this.sessionKey;
        String body;
        try {
            JSONObject data = new JSONObject();
            data.put("settings", settings);
            data.put("workspace_id", workspaceId);
            body = data.toString();
        } catch (JSONException ex) {
            throw new IOException("Unable to serialize text", ex);
        }
        return doPost(url, body);
    }

    public JSONObject applySVMModelToWorkspace(int modelId, int workspaceId) throws IOException {
        String url = this.baseURL + "model/svm/" + modelId + "/apply?session=" + this.sessionKey;
        String body;
        try {
            JSONObject data = new JSONObject();
            data.put("workspace_id", workspaceId);
            body = data.toString();
        } catch (JSONException ex) {
            throw new IOException("Unable to serialize text", ex);
        }
        return doPost(url, body);
    }
    
    public JSONObject applySVMModelToString(int modelId, String text) throws IOException {
        String url = this.baseURL + "model/svm/" + modelId + "/apply?session=" + this.sessionKey;
        String body;
        try {
            JSONObject data = new JSONObject();
            data.put("text", text);
            body = data.toString();
        } catch (JSONException ex) {
            throw new IOException("Unable to serialize text", ex);
        }
        return doPost(url, body);
    }
}
