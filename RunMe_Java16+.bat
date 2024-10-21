java -jar hafen-updater.jar update https://raw.githubusercontent.com/Cediner/ArdClient/Unstable/update/ -Djava.util.logging.config.file=logging.properties
start "" javaw --add-exports=java.desktop/sun.awt=ALL-UNNAMED -Dsun.java2d.uiScale.enabled=false -jar build/hafen.jar
