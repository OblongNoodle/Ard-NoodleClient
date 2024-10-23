package integrations.food;

import com.sun.org.apache.xml.internal.utils.res.XResourceBundle;
import haven.Defer;
import haven.ItemInfo;
import haven.Resource;
import haven.Utils;
import haven.res.ui.tt.q.qbuff.QBuff;
import haven.resutil.FoodInfo;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.json.JSONArray;
import org.json.JSONObject;
import java.io.InputStream;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.FTP;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import org.json.JSONException;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.FileReader;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.nio.file.Files;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;


public class FoodService {
    public static final String API_ENDPOINT = "http://localhost:3000/";
    public static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final String PROJECT_PATH = System.getProperty("user.dir") + "/FoodData/";  // Project directory path
    private static final File FOOD_DATA_CACHE_FILE = new File(PROJECT_PATH + "food_data.json");

    // FTP Configuration
    private static final String FTP_SERVER = "ftp.havengoons.com"; // Replace with your FTP server
    private static final int FTP_PORT = 21; // Default FTP port
    private static final String FTP_USER = "Guest@havengoons.com"; // Replace with your FTP username
    private static final String FTP_PASS = "HavenGoons123"; // Replace with your FTP password



    /**
     * Check item info and determine if it is food and we need to send it
     */
    public static void checkFood(List<ItemInfo> ii, Resource res) {
        if (!Resource.language.equals("en")) {
            return;
        }
        List<ItemInfo> infoList = new ArrayList<>(ii);
        new Thread(() -> {
            try {
                String resName = res.name;
                FoodInfo foodInfo = ItemInfo.find(FoodInfo.class, infoList);
                if (foodInfo != null) {
                    QBuff qBuff = ItemInfo.find(QBuff.class, infoList);
                    double quality = qBuff != null ? qBuff.q : 10.0;
                    double multiplier = Math.sqrt(quality / 10.0);

                    ParsedFoodInfo parsedFoodInfo = new ParsedFoodInfo();
                    parsedFoodInfo.setResourceName(resName);
                    parsedFoodInfo.setEnergy((int) Math.round(foodInfo.end * 100));
                    parsedFoodInfo.setHunger(Math.round(foodInfo.glut * 10000.0) / 100.0); // Adjust to retain decimal precision

                    for (int i = 0; i < foodInfo.evs.length; i++) {
                        parsedFoodInfo.getFeps().add(new FoodFEP(foodInfo.evs[i].ev.orignm, round2Dig(foodInfo.evs[i].a / multiplier)));
                    }

                    for (ItemInfo info : infoList) {
                        if (info instanceof ItemInfo.AdHoc) {
                            String text = ((ItemInfo.AdHoc) info).str.text;
                            if (text.equals("White-truffled") || text.equals("Black-truffled") || text.equals("Peppered")) {
                                return;
                            }
                        }
                        if (info instanceof ItemInfo.Name) {
                            parsedFoodInfo.setItemName(((ItemInfo.Name) info).ostr.text);
                        }
                        if (info.getClass().getName().equals("Ingredient") || info.getClass().getName().equals("Smoke")) {
                            String name = (String) info.getClass().getField("oname").get(info);
                            Double value = (Double) info.getClass().getField("val").get(info);
                            parsedFoodInfo.getIngredients().add(new FoodIngredient(name, (int) (value * 100)));
                        }
                    }

                    // Save parsed food information to file with duplicate check
                    saveFoodDataToFile(parsedFoodInfo);
                }
            } catch (Exception ex) {
                System.out.println("Cannot create food info: " + ex.getMessage());
            }
        }).start();
    }

    private static double round2Dig(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    public static void saveFoodDataToFile(ParsedFoodInfo parsedFoodInfo) {
        try {
            File file = FOOD_DATA_CACHE_FILE;
            JSONArray jsonArray;
            if (file.exists()) {
                BufferedReader reader = new BufferedReader(new FileReader(file));
                StringBuilder existingData = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    existingData.append(line);
                }
                reader.close();
                jsonArray = new JSONArray(existingData.toString());
            } else {
                jsonArray = new JSONArray();
            }

            // Check for duplicates
            if (!isDuplicate(parsedFoodInfo, jsonArray)) {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("itemName", parsedFoodInfo.getItemName());
                jsonObject.put("resourceName", parsedFoodInfo.getResourceName());
                jsonObject.put("energy", parsedFoodInfo.getEnergy());
                jsonObject.put("hunger", parsedFoodInfo.getHunger());

                JSONArray ingredientsArray = new JSONArray();
                for (FoodIngredient ingredient : parsedFoodInfo.getIngredients()) {
                    JSONObject ingredientObject = new JSONObject();
                    ingredientObject.put("name", ingredient.getName());
                    ingredientObject.put("percentage", ingredient.getPercentage());
                    ingredientsArray.put(ingredientObject);
                }
                jsonObject.put("ingredients", ingredientsArray);

                JSONArray fepsArray = new JSONArray();
                for (FoodFEP fep : parsedFoodInfo.getFeps()) {
                    JSONObject fepObject = new JSONObject();
                    fepObject.put("name", fep.getName());
                    fepObject.put("value", fep.getValue());
                    fepsArray.put(fepObject);
                }
                jsonObject.put("feps", fepsArray);

                jsonArray.put(jsonObject);

                PrintWriter writer = new PrintWriter(new FileWriter(file));
                writer.write(jsonArray.toString(4));
                writer.close();
                System.out.println("Food data saved to: " + file.getAbsolutePath());

                // Send JSON data to the website
                sendJsonToHttp(file);
            } else {
                System.out.println("Duplicate food item found, not saving.");
            }
        } catch (IOException e) {
            System.err.println("Error saving food data: " + e.getMessage());
        }
    }

    public static void sendJsonToHttp(File file) {
        try {
            // Read the JSON file content
            String jsonData = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);

            // Create a URL object
            URL url = new URL("https://havengoons.com/food/upload.php");
            HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();

            // Configure HTTP connection
            httpConn.setRequestMethod("POST");
            httpConn.setRequestProperty("Content-Type", "application/json");
            httpConn.setDoOutput(true);  // Allow sending data

            // Send the JSON data
            try (OutputStream os = httpConn.getOutputStream()) {
                byte[] input = jsonData.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            // Get the response code
            int responseCode = httpConn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                System.out.println("File uploaded successfully via HTTP POST.");
            } else {
                System.out.println("Failed to upload the file. HTTP response code: " + responseCode);
            }
        } catch (Exception e) {
            System.err.println("HTTP POST error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static boolean isDuplicate(ParsedFoodInfo parsedFoodInfo, JSONArray jsonArray) {
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject existingItem = jsonArray.getJSONObject(i);
            String existingItemName = existingItem.getString("itemName");
            JSONArray existingIngredients = existingItem.getJSONArray("ingredients");

            // Check if the item name matches
            if (existingItemName.equals(parsedFoodInfo.getItemName())) {
                // Check if the ingredients match
                if (existingIngredients.length() == parsedFoodInfo.getIngredients().size()) {
                    boolean allMatch = true;
                    for (int j = 0; j < existingIngredients.length(); j++) {
                        JSONObject existingIngredient = existingIngredients.getJSONObject(j);
                        String existingIngredientName = existingIngredient.getString("name");
                        int existingIngredientPercentage = existingIngredient.getInt("percentage");

                        FoodIngredient newIngredient = parsedFoodInfo.getIngredients().get(j);
                        if (!existingIngredientName.equals(newIngredient.getName()) || existingIngredientPercentage != newIngredient.getPercentage()) {
                            allMatch = false;
                            break;
                        }
                    }
                    if (allMatch) {
                        return true; // Duplicate found
                    }
                }
            }
        }
        return false; // No duplicates found
    }

    public static class FoodFEP {
        private String name;
        private double value;

        public FoodFEP(String name, double value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public double getValue() {
            return value;
        }
    }

    public static class FoodIngredient {
        private String name;
        private int percentage;

        public FoodIngredient(String name, int percentage) {
            this.name = name;
            this.percentage = percentage;
        }

        public String getName() {
            return name;
        }

        public int getPercentage() {
            return percentage;
        }
    }

    public static class ParsedFoodInfo {
        private String itemName;
        private String resourceName;
        private int energy;
        private double hunger;
        private final List<FoodFEP> feps = new ArrayList<>();
        private final List<FoodIngredient> ingredients = new ArrayList<>();

        public void setItemName(String itemName) {
            this.itemName = itemName;
        }

        public void setResourceName(String resourceName) {
            this.resourceName = resourceName;
        }

        public void setEnergy(int energy) {
            this.energy = energy;
        }

        public void setHunger(double hunger) {
            this.hunger = hunger;
        }

        public String getItemName() {
            return itemName;
        }

        public String getResourceName() {
            return resourceName;
        }

        public int getEnergy() {
            return energy;
        }

        public double getHunger() {
            return hunger;
        }

        public List<FoodFEP> getFeps() {
            return feps;
        }

        public List<FoodIngredient> getIngredients() {
            return ingredients;
        }
    }
}
