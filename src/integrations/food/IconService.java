package integrations.food;

import haven.ItemInfo;
import haven.Resource;
import haven.resutil.FoodInfo;
import org.json.JSONObject;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.logging.Level;

public class IconService {
    private static final Logger LOGGER = Logger.getLogger(IconService.class.getName());
    private static final HashSet<String> sentIcons = new HashSet<>();
    private static final ConcurrentLinkedQueue<IconUpload> uploadQueue = new ConcurrentLinkedQueue<>();

    // Static initializer - this runs when the class is loaded
    static {
        FoodService.scheduler.scheduleAtFixedRate(IconService::processUploadQueue, 10L, 10L, TimeUnit.SECONDS);
    }

    public static void checkIcon(List<ItemInfo> infos, BufferedImage image, Resource res) {
        if (!Resource.language.equals("en")) {
            return;
        }

        try {
            // Check if it's a food item
            FoodInfo foodInfo = ItemInfo.find(FoodInfo.class, infos);
            if (foodInfo == null) {
                return;
            }

            String name = null;
            String resourceName = null;

            // Get food name
            for (ItemInfo info : infos) {
                if (info instanceof ItemInfo.Name) {
                    name = ((ItemInfo.Name) info).str.text;
                    break;
                }
            }

            // Get resource name
            if (res != null) {
                resourceName = res.name;
            }

            // If we have both name and resource name and haven't sent this icon before
            if (name != null && resourceName != null && !sentIcons.contains(name) && image != null) {
                queueIconUpload(name, resourceName, image);
                sentIcons.add(name);
                LOGGER.info("Queued new icon for upload: " + name);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error in checkIcon", e);
        }
    }

    private static void queueIconUpload(String name, String resourceName, BufferedImage icon) {
        uploadQueue.add(new IconUpload(name, resourceName, icon));
    }

    private static void processUploadQueue() {
        IconUpload upload;
        while ((upload = uploadQueue.poll()) != null) {
            try {
                sendIcon(upload.name, upload.resourceName, upload.icon);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to upload icon for: " + upload.name, e);
            }
        }
    }

    private static void sendIcon(String name, String resourceName, BufferedImage icon) {
        try {
            // Create connection
            HttpURLConnection conn = (HttpURLConnection) new URL(FoodService.API_ENDPOINT + "uploadfromclient.php").openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("User-Agent", "H&H Client");
            conn.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
            conn.setDoOutput(true);

            // Convert image to Base64
            ByteArrayOutputStream imageBytes = new ByteArrayOutputStream();
            ImageIO.write(icon, "png", imageBytes);
            imageBytes.flush();
            String base64Image = Base64.getEncoder().encodeToString(imageBytes.toByteArray());

            // Create JSON payload
            JSONObject payload = new JSONObject();
            payload.put("name", name);
            payload.put("resourceName", resourceName);
            payload.put("icon", base64Image);
            payload.put("fileName", resourceName + ".png");

            // Send data
            try (DataOutputStream dos = new DataOutputStream(conn.getOutputStream())) {
                dos.write(payload.toString().getBytes(StandardCharsets.UTF_8));
            }

            // Get response
            int responseCode = conn.getResponseCode();
            LOGGER.info("Sent icon for " + name + " - Response code: " + responseCode);

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error sending icon for: " + name, e);
        }
    }

    private static final class IconUpload {
        public final String name;
        public final String resourceName;
        public final BufferedImage icon;

        public IconUpload(String name, String resourceName, BufferedImage icon) {
            this.name = name;
            this.resourceName = resourceName;
            this.icon = icon;
        }
    }
}
// C