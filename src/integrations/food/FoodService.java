package integrations.food;

import haven.ItemInfo;
import haven.Resource;
import haven.res.ui.tt.q.qbuff.QBuff;
import haven.resutil.FoodInfo;
import org.json.JSONArray;
import org.json.JSONObject;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class FoodService {
    public static final String API_ENDPOINT = "https://havengoons.com/food/";
    public static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

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
                    // Try to get and process icon
                    try {
                        if (res != null) {
                            Resource.Image img = res.layer(Resource.imgc);
                            if (img != null) {
                                BufferedImage rawImage = img.img;  // Use the raw image directly
                                IconService.checkIcon(infoList, rawImage, res);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Error checking icon: " + e.getMessage());
                    }

                    QBuff qBuff = ItemInfo.find(QBuff.class, infoList);
                    double quality = qBuff != null ? qBuff.q : 10.0;
                    double multiplier = Math.sqrt(quality / 10.0);

                    ParsedFoodInfo parsedFoodInfo = new ParsedFoodInfo();
                    parsedFoodInfo.setResourceName(resName);
                    parsedFoodInfo.setEnergy((int) Math.round(foodInfo.end * 100));
                    parsedFoodInfo.setHunger(Math.round(foodInfo.glut * 10000.0) / 100.0);

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

                    // Send parsed food information
                    sendFoodData(parsedFoodInfo);
                }
            } catch (Exception ex) {
                System.out.println("Cannot create food info: " + ex.getMessage());
            }
        }).start();
    }

    private static double round2Dig(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    public static void sendFoodData(ParsedFoodInfo parsedFoodInfo) {
        try {
            // Create JSON object for this food item
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

            // Send single food item to server
            URL url = new URL(API_ENDPOINT + "uploadfromclient.php");
            HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
            httpConn.setRequestMethod("POST");
            httpConn.setRequestProperty("Content-Type", "application/json");
            httpConn.setDoOutput(true);

            try (OutputStream os = httpConn.getOutputStream()) {
                byte[] input = jsonObject.toString(4).getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = httpConn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                System.out.println("Food data uploaded successfully.");
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(httpConn.getInputStream(), StandardCharsets.UTF_8))) {
                    StringBuilder response = new StringBuilder();
                    String responseLine;
                    while ((responseLine = br.readLine()) != null) {
                        response.append(responseLine.trim());
                    }
                    System.out.println("Server response: " + response.toString());
                }
            } else {
                System.out.println("Failed to upload food data. HTTP response code: " + responseCode);
            }
        } catch (Exception e) {
            System.err.println("Error processing food data: " + e.getMessage());
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
                        if (!existingIngredientName.equals(newIngredient.getName()) ||
                                existingIngredientPercentage != newIngredient.getPercentage()) {
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