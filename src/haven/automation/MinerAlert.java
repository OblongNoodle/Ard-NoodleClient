package haven.automation;


import haven.*;
import haven.Button;
import haven.Label;
import haven.Utils;
import haven.Window;
import haven.purus.BotUtils;
import haven.purus.pbot.PBotAPI;

import javax.swing.*;
import java.awt.*;
import java.awt.event.KeyEvent;
import java.util.List;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;

import static haven.OCache.posres;

public class MinerAlert extends Window {
    private GameUI gui;
    private int countiron, countgold, countsilver, countcopper, counttin, countbasaslt, countcinnabar, countdolomite, countfeldspar, countflint, countgneiss, countgranite, counthornblende;
    private int countlimestone, countmarble, countporphyry, countquartz, countsandstone, countschist;
    private int countcassiterite, countchalcopyrite, countmalachite, countilmenite, countlimonite, counthematite, countmagnetite, countgalena, countargentite;
    private int countpetzite, countsylvanite, countnagyagite, counthornsilver,countslimes, countslimestotal;

    private static final int TIMEOUT = 2000;
    private static final int HAND_DELAY = 8;
    private Thread runner;
    public Boolean terminate = false;
    public static int delay = 5000;
    public Gob gob;
    private Button runbtn, stopbtn;
    private final Label labelcountiron, labelcountgold, labelcountsilver, labelcounttin, labelcountcopper, labelcountmagnetite, labelcounthematite,labelcountslimes,labelcountslimestotal;
    private static final Text.Foundry infof = new Text.Foundry(Text.sans, 10).aa(true);
    private double lasterror = 0;
    public List<Gob> slimecount = new ArrayList<>();

    public MinerAlert(GameUI gui) {
        super(new Coord(170, 200), "Miner Alert");
        this.gui = gui;


        final Label labeliron = new Label("Number of Iron tiles visible.", infof);
        add(labeliron, new Coord(10, 8));
        labelcountiron = new Label("0", Text.num12boldFnd, Color.WHITE);
        add(labelcountiron, new Coord(65, 17));

        final Label labeltin = new Label("Number of Tin tiles visible.", infof);
        add(labeltin, new Coord(10, 28));
        labelcounttin = new Label("0", Text.num12boldFnd, Color.WHITE);
        add(labelcounttin, new Coord(65, 37));

        final Label labelcopper = new Label("Number of Copper tiles visible.", infof);
        add(labelcopper, new Coord(10, 48));
        labelcountcopper = new Label("0", Text.num12boldFnd, Color.WHITE);
        add(labelcountcopper, new Coord(65, 57));

        final Label labelgold = new Label("Number of Gold tiles visible.", infof);
        add(labelgold, new Coord(10, 68));
        labelcountgold = new Label("0", Text.num12boldFnd, Color.WHITE);
        add(labelcountgold, new Coord(65, 77));

        final Label labelsilver = new Label("Number of Silver Tiles visible.", infof);
        add(labelsilver, new Coord(10, 88));
        labelcountsilver = new Label("0", Text.num12boldFnd, Color.WHITE);
        add(labelcountsilver, new Coord(65, 97));

        final Label labelmagnetite = new Label("Number of Black Ore Tiles visible.", infof);
        add(labelmagnetite, new Coord(10, 108));
        labelcountmagnetite = new Label("0", Text.num12boldFnd, Color.WHITE);
        add(labelcountmagnetite, new Coord(65, 117));

        final Label labelhematite = new Label("Number of Bloodstone Tiles visible.", infof);
        add(labelhematite, new Coord(10, 128));
        labelcounthematite = new Label("0", Text.num12boldFnd, Color.WHITE);
        add(labelcounthematite, new Coord(65, 137));

        final Label labelslimes = new Label("Number of Slimes Visible", infof);
        add(labelslimes, new Coord(10, 148));
        labelcountslimes = new Label("0", Text.num12boldFnd, Color.WHITE);
        add(labelcountslimes, new Coord(65, 157));

        final Label labelslimestotal = new Label("Number of Slimes Total", infof);
        add(labelslimestotal, new Coord(10, 168));
        labelcountslimestotal = new Label("0", Text.num12boldFnd, Color.WHITE);
        add(labelcountslimestotal, new Coord(65, 177));


        runbtn = new Button(100, "Run") {
            @Override
            public void click() {
                // this.hide();
                // cbtn.show();
                // stopbtn.show();
                terminate = false;
                // labelcountiron.settext("");
                runner = new Thread(new MinerAlert.runner(), "Miner Alert");
                runner.start();
            }
        };
       // add(runbtn, new Coord(35, 440));

        stopbtn = new Button(100, "Stop") {
            @Override
            public void click() {
                //BotUtils.sysMsg("Stopping",Color.white);
                // this.hide();
                // runbtn.show();
                cbtn.show();
                terminate = true;
            }
        };
        //add(stopbtn, new Coord(35, 470));

        ActionListener timedevent = new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                runbtn.click();
            }
        };  new javax.swing.Timer(delay,timedevent).start();

    }



    private class runner implements Runnable {
        @Override
        public void run() {
            while (!terminate) {
                countiron = 0;
                countgold = 0;
                countsilver = 0;
                MCache mcache = gui.map.glob.map;
                Glob g = gui.map.glob;
                Gob player = gui.map.player();
                List<Gob> allGobs = PBotAPI.getGobs();
                List<Gob> list = new ArrayList<>();


                for (int i = 0; i < allGobs.size(); i++) {
                    try {
                        Resource res = allGobs.get(i).getres();
                        if (allGobs.get(i).getres().name.endsWith("greenooze") && !allGobs.get(i).knocked) {
                            list.add(allGobs.get(i));
                            if (!slimecount.contains(allGobs.get(i)))
                                slimecount.add(allGobs.get(i));
                        }
                    }
                    catch(NullPointerException | Loading e){ }
                }
                countslimes = list.size();
                // Gob player = ((plgob < 0) ? null : glob.oc.getgob(plgob));
                Coord pltc = new Coord((int) player.getc().x / 11, (int) player.getc().y / 11);

                for (int x = -44; x < 44; x++) {
                    for (int y = -44; y < 44; y++) {
                        int t = g.map.gettile(pltc.sub(x, y));
                        Resource res = g.map.tilesetr(t);
                        if (res == null)
                            continue;

                        String name = res.name;
                        if (name.equals("gfx/tiles/rocks/cassiterite")) {
                            counttin = counttin + 1;
                            countcassiterite = countcassiterite + 1;
                        }
                        if (name.equals("gfx/tiles/rocks/chalcopyrite")) {
                            countiron = countiron + 1;
                            countcopper = countcopper + 1;
                            countchalcopyrite = countchalcopyrite + 1;
                        }
                        if (name.equals("gfx/tiles/rocks/malachite")) {
                            countcopper = countcopper + 1;
                            countmalachite = countmalachite + 1;
                        }
                        if (name.equals("gfx/tiles/rocks/ilmenite")) {
                            countiron = countiron + 1;
                            countilmenite = countilmenite + 1;
                        }
                        if (name.equals("gfx/tiles/rocks/limonite")) {
                            countiron = countiron + 1;
                            countlimonite = countlimonite + 1;
                        }
                        if (name.equals("gfx/tiles/rocks/hematite")) {
                            countiron = countiron + 1;
                            counthematite = counthematite + 1;
                        }
                        if (name.equals("gfx/tiles/rocks/magnetite")) {
                            countiron = countiron + 1;
                            countmagnetite = countmagnetite + 1;
                        }
                        if (name.equals("gfx/tiles/rocks/galena")) {
                            countsilver = countsilver + 1;
                            countgalena = countgalena + 1;
                        }
                        if (name.equals("gfx/tiles/rocks/argentite")) {
                            countsilver = countsilver + 1;
                            countargentite = countargentite + 1;
                        }
                        if (name.equals("gfx/tiles/rocks/hornsilver")) {
                            countsilver = countsilver + 1;
                            counthornsilver = counthornsilver + 1;
                        }
                        if (name.equals("gfx/tiles/rocks/petzite")) {
                            countgold = countgold + 1;
                            countpetzite = countpetzite + 1;
                        }
                        if (name.equals("gfx/tiles/rocks/sylvanite")) {
                            countgold = countgold + 1;
                            countsylvanite = countsylvanite + 1;
                        }
                        if (name.equals("gfx/tiles/rocks/nagyagite")) {
                            countgold = countgold + 1;
                            countnagyagite = countnagyagite + 1;
                        }

                    }
                }
                labelcountiron.settext(countiron + "");
                labelcountcopper.settext(countcopper + "");
                labelcounttin.settext(counttin + "");
                labelcountgold.settext(countgold + "");
                labelcountsilver.settext(countsilver + "");
                labelcountmagnetite.settext(countmagnetite + "");
                labelcounthematite.settext(counthematite + "");
                labelcountslimes.settext(countslimes + "");
                labelcountslimestotal.settext(slimecount.size() + "");
                if(countgold > 0) {
                    double now = Utils.rtime();
                    if(now-lasterror > 15) {
                        lasterror = now;
                        gui.error("Gold Visible on Screen");
                    }
                }
                if(countsilver > 0) {
                    double now = Utils.rtime();
                    if (now-lasterror>15) {
                        gui.error("Silver Visible on Screen");
                        lasterror = now;
                    }
                }
                if(countslimes>0) {
                    double now = Utils.rtime();
                    if(now-lasterror > 15) {
                        gui.error("Slime number spawned : " + list.size());
                        lasterror = now;
                    }
                }
                countiron = 0;
                counttin = 0;
                countcopper = 0;
                countgold = 0;
                countsilver = 0;
                counthematite = 0;
                countmagnetite = 0;
                countslimes=0;
                list = null;
                stopbtn.click();
            }
        }
    }

    @Override
    public void wdgmsg(Widget sender, String msg, Object... args) {
        if (sender == cbtn)
            reqdestroy();
        else
            super.wdgmsg(sender, msg, args);
    }
    @Override
    public boolean type(char key, KeyEvent ev) {
        if (key == 27) {
            if (cbtn.visible)
                reqdestroy();
            return true;
        }
        return super.type(key, ev);
    }
    public void terminate() {
        terminate = true;
        if (runner != null)
            runner.interrupt();
        this.destroy();
    }
}
