/*
 *  This file is part of the Haven & Hearth game client.
 *  Copyright (C) 2009 Fredrik Tolf <fredrik@dolda2000.com>, and
 *                     Björn Johannessen <johannessen.bjorn@gmail.com>
 *
 *  Redistribution and/or modification of this file is subject to the
 *  terms of the GNU Lesser General Public License, version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  Other parts of this source tree adhere to other copying
 *  rights. Please see the file `COPYING' in the root directory of the
 *  source tree for details.
 *
 *  A copy the GNU Lesser General Public License is distributed along
 *  with the source tree of which this file is a part in the file
 *  `doc/LPGL-3'. If it is missing for any reason, please see the Free
 *  Software Foundation's website at <http://www.fsf.org/>, or write
 *  to the Free Software Foundation, Inc., 59 Temple Place, Suite 330,
 *  Boston, MA 02111-1307 USA
 */

package haven;

import haven.Defer.Future;
import haven.resutil.Ridges;
import modification.configuration;
import modification.dev;
import modification.resources;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Shape;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import static haven.DefSettings.MAPTYPE;
import static haven.MCache.cmaps;

public class MapFile {
    private static MapFile instance = null;

    // You need multiple grids around one otherwise it can merge!
    @Deprecated
    public void updategrids(MCache map, Collection<MCache.Grid> grids) {
        if (!grids.isEmpty()) {
            synchronized (procmon) {
                updqueue.add(new Pair<>(map, grids));
                process();
            }
        }
    }

    public void removeSegment(MapFileWidget.Location loc) {
        lock.writeLock().lock();
        segments.remove(loc.seg.id);
        Path mf = Utils.pj(HashDirCache.findbase(), mangle(String.format("seg-%x", loc.seg.id)));
        try {
            Files.deleteIfExists(mf);
        } catch (Exception ignored) {
        }
        lock.writeLock().unlock();
    }

    @Deprecated
    public static Resource loadsaved(Resource.Pool pool, Resource.Spec spec) {
        try {
            return (spec.get());
        } catch (Loading l) {
            throw (l);
        } catch (Exception e) {
            return (pool.load(spec.name).get());
        }
    }

    public static boolean debug = Utils.getprefb("mapdebug", false);
    public final ResCache store;
    public final String filename;
    public final Collection<Long> knownsegs = new HashSet<>();
    public final Collection<Marker> markers = new ArrayList<>();
    public final Map<Long, SMarker> smarkers = new HashMap<>();
    public int markerseq = 0;
    public final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Random rnd = new Random();

    public MapFile(ResCache store, String filename) {
        this.store = store;
        this.filename = filename;
    }

    private void checklock() {
        if ((lock.getReadHoldCount() == 0) && !lock.isWriteLockedByCurrentThread())
            throw (new IllegalMonitorStateException());
    }

    private String mangle(String datum) {
        StringBuilder buf = new StringBuilder();
        buf.append("map/");
        if (!filename.equals("")) {
            buf.append(filename);
            buf.append('/');
        }
        buf.append(datum);
        return (buf.toString());
    }

    private final List<Pair<String, Object>> sync = new ArrayList<>();

    private InputStream sfetch(String ctl, Object... args) throws IOException {
        String name = mangle(String.format(ctl, args));
        final Pair<String, Object> pp;
        synchronized (sync) {
            pp = sync.stream().filter(p -> p.a.equals(name)).findFirst().orElseGet(() -> {
                Pair<String, Object> p = new Pair<>(name, new Object());
                sync.add(p);
                return (p);
            });
        }
        synchronized (pp.b) {
            return (store.fetch(name));
        }
    }

    private OutputStream sstore(String ctl, Object... args) throws IOException {
        String name = mangle(String.format(ctl, args));
        final Pair<String, Object> pp;
        synchronized (sync) {
            pp = sync.stream().filter(p -> p.a.equals(name)).findFirst().orElseGet(() -> {
                Pair<String, Object> p = new Pair<>(name, new Object());
                sync.add(p);
                return (p);
            });
        }
        synchronized (pp.b) {
            return (store.store(name));
        }
    }

    public static void warn(Throwable cause, String msg) {
        if (debug)
            Debug.log.printf("mapfile warning: %s%n", msg);
        new Warning(cause, msg).issue();
    }

    public static void warn(Throwable cause, String fmt, Object... args) {
        warn(cause, String.format(fmt, args));
    }

    public static void warn(String fmt, Object... args) {
        warn(null, fmt, args);
    }

    public static synchronized MapFile load(ResCache store, String filename) throws IOException {
        if (instance != null)
            return instance;
        else {
            final MapFile file = new MapFile(store, filename);
            InputStream fp;
            try {
                fp = file.sfetch("index");
            } catch (FileNotFoundException e) {
                instance = file;
                return (file);
            }
            try (StreamMessage data = new StreamMessage(fp)) {
                int ver = data.uint8();
                if (ver == 1) {
                    for (int i = 0, no = data.int32(); i < no; i++)
                        file.knownsegs.add(data.int64());
                    for (int i = 0, no = data.int32(); i < no; i++) {
                        try {
                            Marker mark = loadmarker(data);
                            file.markers.add(mark);
                            if (mark instanceof SMarker)
                                file.smarkers.put(((SMarker) mark).oid, (SMarker) mark);
                        } catch (Message.BinError e) {
                            warn("mapfile warning: error when loading marker, data may be missing: %s", e);
                        }
                    }
                } else {
                    throw (new IOException(String.format("unknown mapfile index version: %d", ver)));
                }
            } catch (Message.BinError e) {
                throw (new IOException(String.format("error when loading index: %s", e), e));
            }
            instance = file;
            return (file);
        }
    }

    private void save() {
        checklock();
        OutputStream fp;
        try {
            fp = sstore("index");
        } catch (IOException e) {
            throw (new StreamMessage.IOError(e));
        }
        try (StreamMessage out = new StreamMessage(fp)) {
            out.adduint8(1);
            out.addint32(knownsegs.size());
            for (Long seg : knownsegs)
                out.addint64(seg);
            out.addint32(markers.size());
            for (Marker mark : markers)
                savemarker(out, mark);
            Utils.saveCustomList(resources.customSendMarks, "CustomSendMarks");
        }
    }

    public void defersave() {
        synchronized (procmon) {
            gdirty = true;
            process();
        }
    }

    public static class GridInfo {
        public final long id, seg;
        public final Coord sc;

        public GridInfo(long id, long seg, Coord sc) {
            this.id = id;
            this.seg = seg;
            this.sc = sc;
        }
    }

    public final BackCache<Long, GridInfo> gridinfo = new BackCache<>(100, id -> {
        checklock();
        InputStream fp;
        try {
            fp = sfetch("gi-%x", id);
        } catch (IOException e) {
            return (null);
        }
        try (StreamMessage data = new StreamMessage(fp)) {
            int ver = data.uint8();
            if (ver == 1) {
                return (new GridInfo(data.int64(), data.int64(), data.coord()));
            } else {
                throw (new Message.FormatError("Unknown gridinfo version: " + ver));
            }
        } catch (Message.BinError e) {
            warn(e, "error when loading gridinfo for %x: %s", id, e);
            return (null);
        }
    }, (id, info) -> {
        checklock();
        OutputStream fp;
        try {
            fp = sstore("gi-%x", info.id);
        } catch (IOException e) {
            throw (new StreamMessage.IOError(e));
        }
        try (StreamMessage out = new StreamMessage(fp)) {
            out.adduint8(1);
            out.addint64(info.id);
            out.addint64(info.seg);
            out.addcoord(info.sc);
        }
    });

    private static Runnable locked(Runnable r, Lock lock) {
        return (() -> {
            lock.lock();
            try {
                r.run();
            } finally {
                lock.unlock();
            }
        });
    }

    private static <A, R> Function<A, R> locked(Function<A, R> f, Lock lock) {
        return (v -> {
            lock.lock();
            try {
                return (f.apply(v));
            } finally {
                lock.unlock();
            }
        });
    }

    private final Object procmon = new Object();
    private Thread processor = null;
    private final Collection<Pair<MCache, Collection<MCache.Grid>>> updqueue = new HashSet<>();
    private final Collection<Segment> dirty = new HashSet<>();
    private boolean gdirty = false;

    private class Processor extends HackThread {
        Processor() {
            super("Mapfile processor");
            setDaemon(true);
        }

        public void run() {
            try {
                long last = System.currentTimeMillis();
                while (true) {
                    Runnable task;
                    long now = System.currentTimeMillis();
                    synchronized (procmon) {
                        if (!updqueue.isEmpty()) {
                            Pair<MCache, Collection<MCache.Grid>> el = Utils.take(updqueue);
                            task = () -> MapFile.this.update(el.a, el.b);
                        } else if (!dirty.isEmpty()) {
                            Segment seg = Utils.take(dirty);
                            task = locked(() -> segments.put(seg.id, seg), lock.writeLock());
                        } else if (gdirty) {
                            task = locked(MapFile.this::save, lock.readLock());
                            gdirty = false;
                        } else {
                            if (now - last > 10000) {
                                processor = null;
                                return;
                            }
                            procmon.wait(5000);
                            continue;
                        }
                    }
                    task.run();
                    last = now;
                }
            } catch (InterruptedException e) {
            } catch (Throwable e) {
                Debug.printStackTrace(e);
            } finally {
                synchronized (procmon) {
                    processor = null;
                }
            }
        }
    }

    private void process() {
        synchronized (procmon) {
            if (processor == null) {
                Thread np = new Processor();
                np.start();
                processor = np;
            }
            procmon.notifyAll();
        }
    }

    public abstract static class Marker {
        public long seg;
        public Coord tc;
        public String nm;

        public Marker(long seg, Coord tc, String nm) {
            this.seg = seg;
            this.tc = tc;
            this.nm = nm;
        }

        public String name() {
            return nm;
        }

        public String tip() {
            return nm;
        }
    }

    public static class PMarker extends Marker {
        public Color color;

        public PMarker(long seg, Coord tc, String nm, Color color) {
            super(seg, tc, nm);
            this.color = color;
        }
    }

    public static class SMarker extends Marker {
        public long oid;
        public Resource.Spec res;
        public boolean autosend = true;

        public SMarker(long seg, Coord tc, String nm, long oid, Resource.Spec res) {
            super(seg, tc, nm);
            this.oid = oid;
            this.res = res;
        }

        public void makeAutosend(boolean val) {
            this.autosend = val;
            resources.customSendMarks.put(Long.toString(oid), val);
        }
    }

    private static Marker loadmarker(Message fp) {
        int ver = fp.uint8();
        if (ver == 1) {
            long seg = fp.int64();
            Coord tc = fp.coord();
            String nm = fp.string();
            char type = (char) fp.uint8();
            switch (type) {
                case 'p':
                    Color color = fp.color();
                    return (new PMarker(seg, tc, nm, color));
                case 's':
                    long oid = fp.int64();
                    Resource.Spec res = new Resource.Spec(Resource.remote(), fp.string(), fp.uint16());
                    SMarker sm = new SMarker(seg, tc, nm, oid, res);
                    Boolean st = resources.customSendMarks.get(Long.toString(oid));
                    if (st != null)
                        sm.makeAutosend(st);
                    return (sm);
                default:
                    throw (new Message.FormatError("Unknown marker type: " + (int) type));
            }
        } else {
            throw (new Message.FormatError("Unknown marker version: " + ver));
        }
    }

    private static void savemarker(Message fp, Marker mark) {
        fp.adduint8(1);
        fp.addint64(mark.seg);
        fp.addcoord(mark.tc);
        fp.addstring(mark.nm);
        if (mark instanceof PMarker) {
            fp.adduint8('p');
            fp.addcolor(((PMarker) mark).color);
        } else if (mark instanceof SMarker) {
            SMarker sm = (SMarker) mark;
            fp.adduint8('s');
            fp.addint64(sm.oid);
            fp.addstring(sm.res.name);
            fp.adduint16(sm.res.ver);
        } else {
            throw (new ClassCastException("Can only save PMarkers and SMarkers"));
        }
    }

    public void add(Marker mark) {
        lock.writeLock().lock();
        try {
            if (markers.add(mark)) {
                if (mark instanceof SMarker)
                    smarkers.put(((SMarker) mark).oid, (SMarker) mark);
                defersave();
                markerseq++;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void remove(Marker mark) {
        lock.writeLock().lock();
        try {
            if (markers.remove(mark)) {
                if (mark instanceof SMarker) {
                    resources.customSendMarks.remove(Long.toString(((SMarker) mark).oid));
                    smarkers.remove(((SMarker) mark).oid, mark);
                }
                defersave();
                markerseq++;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void update(Marker mark) {
        lock.readLock().lock();
        try {
            if (markers.contains(mark)) {
                defersave();
                markerseq++;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public static class TileInfo {
        public final Resource.Spec res;
        public final int prio;

        public TileInfo(Resource.Spec res, int prio) {
            this.res = res;
            this.prio = prio;
        }
    }

    public static class Overlay {
        public final Resource.Spec olid;
        public final boolean[] ol;

        public Overlay(Resource.Spec olid, boolean[] ol) {
            this.olid = olid;
            this.ol = ol;
        }

        public boolean get(Coord c) {
            return (ol[c.x + (c.y * cmaps.x)]);
        }
    }

    public static class DataGrid {
        public final TileInfo[] tilesets;
        public final int[] tiles;
        public final float[] zmap;
        public final Collection<Overlay> ols = new ArrayList<>();
        public final long mtime;
        private static final Map<BufferedImage, Color> simple_textures = Collections.synchronizedMap(new WeakHashMap<>());
        private static final Map<String, BufferedImage> texes = Collections.synchronizedMap(new WeakHashMap<>());

        public DataGrid(TileInfo[] tilesets, int[] tiles, float[] zmap, long mtime) {
            this.tilesets = tilesets;
            this.tiles = tiles;
            this.zmap = zmap;
            this.mtime = mtime;
            configuration.addTiles(tilesets);
        }

        public int gettile(Coord c) {
            return (tiles[c.x + (c.y * cmaps.x)]);
        }

        public int gettile(String name) {
            for (int i = 0; i < tilesets.length; i++) {
                if (tilesets[i].res.name().equalsIgnoreCase(name))
                    return i;
            }
            return -1;
        }

        public double getfz(Coord c) {
            return (zmap[c.x + (c.y * cmaps.x)]);
        }

        private BufferedImage tiletex(int t, BufferedImage[] texes, boolean[] cached) {
            if (!cached[t]) {
                Resource r = null;
                try {
                    r = tilesets[t].res.loadsaved(Resource.remote());
                } catch (Loading l) {
                    throw (l);
                } catch (Exception e) {
                    warn(e, "could not load tileset resource %s(v%d): %s", tilesets[t].res.name, tilesets[t].res.ver, e);
                }
                if (r != null) {
                    Resource.Image ir = r.layer(Resource.imgc);
                    if (ir != null) {
                        texes[t] = ir.img;
                    }
                }
                cached[t] = true;
            }
            return (texes[t]);
        }

        private BufferedImage tiletex(int t, BufferedImage[] texes, boolean[] cached, String res) {
            if (cached[t])
                return texes[t];
            else {
                Resource r = null;
                try {
                    r = Resource.remote().loadwait(res);
                } catch (Loading l) {
                    throw (l);
                } catch (Exception e) {
                    warn("could not load tileset resource %s: %s", res, e);
                }
                if (r != null) {
                    Resource.Image ir = r.layer(Resource.imgc);
                    TexR tr = r.layer(TexR.class);
                    if (ir != null) {
                        texes[t] = ir.img;
                    } else if (tr != null) {
                        texes[t] = tr.tex.fill();
                    }
                }
                cached[t] = true;
                return texes[t];
            }
        }

        private static final BufferedImage EMPTY_TILE = new BufferedImage(1, 1, 1);

        private static BufferedImage tiletex(int t, TileInfo[] tilesets) {
            Resource.Spec res = tilesets[t].res;
            BufferedImage img = texes.get(res.name());
            if (img == null) {
                Resource r = null;
                try {
                    r = res.loadsaved(Resource.remote());
                } catch (Loading l) {
                    throw (l);
                } catch (Exception e) {
                    warn(e, "could not load tileset resource %s(v%d): %s", res.name, res.ver, e);
                }
//                synchronized (texes) {
                    img = texes.get(res.name());
                    if (img == null) {
                        if (r != null) {
                            Resource.Image ir = r.layer(Resource.imgc);
                            if (ir != null) {
                                img = ir.img;
                            }
                        }
                    }
                    if (img == null)
                        img = EMPTY_TILE;
                    texes.put(res.name(), img);
//                }
            }
            return (img == EMPTY_TILE ? null : img);
        }

        private static BufferedImage tiletex(int t, TileInfo[] tilesets, String res) {
            BufferedImage img = tiletex(t, tilesets);
            if (img == null) {
                Resource r = null;
                try {
                    r = Resource.remote().loadwait(res);
                } catch (Loading l) {
                    throw (l);
                } catch (Exception e) {
                    warn("could not load tileset resource %s: %s", res, e);
                }
//                synchronized (texes) {
                    img = texes.get(res);
                    if (img == null) {
                        if (r != null) {
                            Resource.Image ir = r.layer(Resource.imgc);
                            TexR tr = r.layer(TexR.class);
                            if (ir != null) {
                                img = ir.img;
                                texes.put(res, img);
                            } else if (tr != null) {
                                img = tr.tex.fill();
                            } else {
                                dev.simpleLog("Not found " + res);
                            }
                        }
                    }
                    if (img == null)
                        img = EMPTY_TILE;
                    texes.put(res, img);
//                }
            }
            return (img == EMPTY_TILE ? null : img);
        }

        public boolean hasTiles(String... tilenames) {
            Coord c = new Coord();
            for (c.y = 0; c.y < cmaps.y; c.y++) {
                for (c.x = 0; c.x < cmaps.x; c.x++) {
                    for (String tnm : tilenames) {
                        if (gettile(tnm) != -1) return (true);
                    }
                }
            }
            return (false);
        }

        public BufferedImage highlightOverlay(String... tilenames) {
            WritableRaster buf = PUtils.imgraster(cmaps);
            Coord c = new Coord();
            for (c.y = 0; c.y < buf.getHeight(); c.y++) {
                for (c.x = 0; c.x < buf.getWidth(); c.x++) {
                    final int t = gettile(c);
                    final String tname = tileName(t);
                    for (String tnm : tilenames) {
                        if (tname.equalsIgnoreCase(tnm)) {
                            buf.setSample(c.x, c.y, 0, 255);
                            buf.setSample(c.x, c.y, 1, 255);
                            buf.setSample(c.x, c.y, 2, 255);
                            buf.setSample(c.x, c.y, 3, 255);
                            break;
                        }
                    }
                }
            }
            return (PUtils.rasterimg(buf));
        }

        public BufferedImage render(Coord off) {
//            BufferedImage[] texes = new BufferedImage[tilesets.length];
//            boolean[] cached = new boolean[tilesets.length];
            WritableRaster buf = PUtils.imgraster(cmaps);
            Coord c = new Coord();
            if (configuration.allowtexturemap) {
                for (c.y = 0; c.y < cmaps.y; c.y++) {
                    for (c.x = 0; c.x < cmaps.x; c.x++) {
                        int t = gettile(c);
                        final String tname = tileName(t);
                        BufferedImage tex;
                        if (configuration.cavetileonmap && isContains(t, "gfx/tiles/rocks/")) {
                            final String newtype = "gfx/terobjs/bumlings/" + tname.substring(tname.lastIndexOf("/") + 1);
                            tex = tiletex(t, tilesets, newtype);
                        } else tex = tiletex(t, tilesets);
                        int rgb = 0;
                        if (tex != null) {
                            switch (MAPTYPE.get()) {
                                case 1:
                                    rgb = tex.getRGB(Utils.floormod(c.x + off.x, tex.getWidth()), Utils.floormod(c.y + off.y, tex.getHeight()));

                                    int mixrgb = tex.getRGB(20, 45);

                                    Color mixtempColor = new Color(mixrgb, true);
                                    Color tempColor = new Color(rgb, true);

                                    tempColor = Utils.blendcol(tempColor, mixtempColor, configuration.simplelmapintens);
                                    rgb = tempColor.getRGB();
                                    break;
                                case 2:
                                    Color simple_color = simple_tile_img(tex);
                                    if (simple_color != null)
                                        rgb = simple_color.getRGB();
                                    break;
                                default:
                                    rgb = tex.getRGB(Utils.floormod(c.x + off.x, tex.getWidth()), Utils.floormod(c.y + off.y, tex.getHeight()));
                                    break;
                            }
                        }

                        buf.setSample(c.x, c.y, 0, (rgb & 0x00ff0000) >>> 16);
                        buf.setSample(c.x, c.y, 1, (rgb & 0x0000ff00) >>> 8);
                        buf.setSample(c.x, c.y, 2, (rgb & 0x000000ff) >>> 0);
                        buf.setSample(c.x, c.y, 3, (rgb & 0xff000000) >>> 24);
                    }
                }
            }
            if (configuration.allowoutlinemap) {
                for (c.y = 0; c.y < cmaps.y; c.y++) {
                    for (c.x = 0; c.x < cmaps.x; c.x++) {
                        int p = tilesets[gettile(c)].prio;
                        int t = gettile(c);
                        if (!(configuration.disablepavingoutlineonmap && isContains(t, "gfx/tiles/paving/"))) {
                            for (Coord ec : configuration.anotheroutlinemap ? cset : tecs) {
                                Coord coord = c.add(ec);
                                if (coord.x < 0 || coord.x > cmaps.x - 1 || coord.y < 0 || coord.y > cmaps.y - 1)
                                    continue;
                                if (tilesets[gettile(coord)].prio > p) {
                                    buf.setSample(c.x, c.y, 0, 0);
                                    buf.setSample(c.x, c.y, 1, 0);
                                    buf.setSample(c.x, c.y, 2, 0);
                                    buf.setSample(c.x, c.y, 3, configuration.mapoutlinetransparency);
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            if (zmap[0] != Float.POSITIVE_INFINITY) {//FIXME
                Tiler[] tilers = new Tiler[tilesets.length];
                boolean[] tlcached = new boolean[tilesets.length];
                if (configuration.allowridgesmap) {
                    for (c.y = 0; c.y < cmaps.y; c.y++) {
                        for (c.x = 0; c.x < cmaps.x; c.x++) {
                            final Tiler t = tiler(gettile(c), tilers, tlcached);
                            if (t instanceof Ridges.RidgeTile && brokenp(t, c, tilers, tlcached)) {
                                final Color black = Color.BLACK;
                                buf.setSample(c.x, c.y, 0, black.getRed());
                                buf.setSample(c.x, c.y, 1, black.getGreen());
                                buf.setSample(c.x, c.y, 2, black.getBlue());
                                buf.setSample(c.x, c.y, 3, black.getAlpha());

                                if (configuration.allowoutlinemap) {
                                    for (Coord ec : configuration.anotheroutlinemap ? cset : tecs) {
                                        Coord coord = c.add(ec);
                                        if (coord.x < 0 || coord.x > cmaps.x - 1 || coord.y < 0 || coord.y > cmaps.y - 1)
                                            continue;
                                        Color cc = new Color(buf.getSample(coord.x, coord.y, 0), buf.getSample(coord.x, coord.y, 1), buf.getSample(coord.x, coord.y, 2), buf.getSample(coord.x, coord.y, 3));
                                        final Color blended = Utils.blendcol(cc, Color.BLACK, 0.1 * (configuration.mapoutlinetransparency / 255.0));
                                        buf.setSample(coord.x, coord.y, 0, blended.getRed());
                                        buf.setSample(coord.x, coord.y, 1, blended.getGreen());
                                        buf.setSample(coord.x, coord.y, 2, blended.getBlue());
                                        buf.setSample(coord.x, coord.y, 3, blended.getAlpha());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return (PUtils.rasterimg(buf));
        }

        private static Color olcol(MCache.OverlayInfo olid) {
            /* XXX? */
            Material mat = olid.mat();
            FColor bc = null;
            for (GLState state : mat.states) {
                if (state instanceof States.ColState) {
                    States.ColState col = (States.ColState) state;
                    bc = new FColor(col.c);
                } else if (state instanceof Material.Colors) {
                    Material.Colors col = (Material.Colors) state;
                    bc = new FColor(col.emi[0], col.emi[1], col.emi[2]);
                }
            }
            return (bc != null ? new Color(Math.round(bc.r * 255), Math.round(bc.g * 255), Math.round(bc.b * 255), 255) : (null));
        }

        private static Color olcol(Collection<String> tags) {
            if (tags.contains("vlg")) {
                if (tags.contains("own"))
                    return (new Color(0, 0, 255));
                else if (tags.contains("!own"))
                    return (new Color(128, 0, 255));
            } else if (tags.contains("cplot")) {
                if (tags.contains("own"))
                    return (new Color(255, 0, 128));
                else if (tags.contains("!own"))
                    return (new Color(255, 0, 0));
            }
            return (null);
//            new Color(255, 0, 128, 32);
//            new Color(0, 0, 255, 32);
//            new Color(255, 0, 0, 32);
//            new Color(128, 0, 255, 32);
//            new Color(255, 255, 255, 32);
//            new Color(0, 255, 128, 32);
//            new Color(0, 0, 0, 64);
//            new Color(0, 255, 0, 32);
//            new Color(255, 255, 0, 32);
//            new Color(29, 196, 51, 60);
        }

        public BufferedImage olrenderfog(final List<Integer> points) {
            WritableRaster buf = PUtils.imgraster(cmaps);

            final double size = 100.0 / 11.0;

            final BufferedImage img = PUtils.rasterimg(buf);
            final Graphics2D g = img.createGraphics();
            final Shape rect = new Rectangle2D.Double(0, 0, size, size);

            if (points.contains(-1)) {
                final Coord cc = Coord.z;
                g.setColor(Color.WHITE);
                g.translate(cc.x * size, cc.y * size);
                g.scale(11.0, 11.0);
                g.fill(rect);
                g.translate(-(cc.x * size), -(cc.y * size));
            } else {
                for (int i : points) {
                    final int x = i % 11;
                    final int y = i / 11;
                    g.setColor(Color.WHITE);
                    g.translate(x * size, y * size);
                    g.fill(rect);
                    g.translate(-(x * size), -(y * size));
                }
            }
            return (img);
        }

        public BufferedImage olrender(Coord off, String tag) {
            WritableRaster buf = PUtils.imgraster(cmaps);
            for (Overlay ol : ols) {
                MCache.ResOverlay olid = ol.olid.loadsaved().flayer(MCache.ResOverlay.class);
                String ovl = null;
                Iterator<String> i = olid.tags().iterator();
                while (i.hasNext()) {
                    ovl = i.next();
                    break;
                }
                if (ovl == null || !ovl.equals(tag))
                    continue;
                Color col = olcol(olid);
                if (col == null)
                    col = olcol(olid.tags());
                if (col == null)
                    continue;
                Coord c = new Coord();
                for (c.y = 0; c.y < cmaps.y; c.y++) {
                    for (c.x = 0; c.x < cmaps.x; c.x++) {
                        if (ol.get(c)) {
                            buf.setSample(c.x, c.y, 0, ((col.getRed() * col.getAlpha()) + (buf.getSample(c.x, c.y, 1) * (255 - col.getAlpha()))) / 255);
                            buf.setSample(c.x, c.y, 1, ((col.getGreen() * col.getAlpha()) + (buf.getSample(c.x, c.y, 1) * (255 - col.getAlpha()))) / 255);
                            buf.setSample(c.x, c.y, 2, ((col.getBlue() * col.getAlpha()) + (buf.getSample(c.x, c.y, 2) * (255 - col.getAlpha()))) / 255);
                            buf.setSample(c.x, c.y, 3, Math.max(buf.getSample(c.x, c.y, 3), col.getAlpha()));
                        }
                    }
                }
            }
            return (PUtils.rasterimg(buf));
        }

        public static void savetiles(Message fp, TileInfo[] tilesets, int[] tiles) {
            fp.adduint16(tilesets.length);
            for (TileInfo tileset : tilesets) {
                fp.addstring(tileset.res.name);
                fp.adduint16(tileset.res.ver);
                fp.adduint8(tileset.prio);
            }
            if (tilesets.length <= 256) {
                for (int tn : tiles)
                    fp.adduint8(tn);
            } else {
                for (int tn : tiles)
                    fp.adduint16(tn);
            }
        }

        public static Pair<TileInfo[], int[]> loadtiles(Message fp, int ver) {
            TileInfo[] tilesets = new TileInfo[(ver >= 2) ? fp.uint16() : fp.uint8()];
            for (int i = 0; i < tilesets.length; i++)
                tilesets[i] = new TileInfo(new Resource.Spec(Resource.remote(), fp.string(), fp.uint16()), fp.uint8());
            int[] tiles = new int[cmaps.x * cmaps.y];
            if (tilesets.length <= 256) {
                for (int i = 0; i < cmaps.x * cmaps.y; i++)
                    tiles[i] = fp.uint8();
            } else {
                for (int i = 0; i < cmaps.x * cmaps.y; i++)
                    tiles[i] = fp.uint16();
            }
            return (new Pair<>(tilesets, tiles));
        }

        public static void savez(Message fp, float[] zmap) {
            float min = zmap[0], max = zmap[0];
            for (float z : zmap) {
                min = Math.min(z, min);
                max = Math.max(z, max);
            }
            if (min == max) {
                fp.adduint8(0);
                fp.addfloat32(min);
                return;
            }
            quantize:
            {
                float q = 0, E = 0.01f;
                for (float z : zmap) {
                    if (z > (min + E)) {
                        if (q == 0)
                            q = z - min;
                        else
                            q = Utils.gcd(q, z - min, E);
                    }
                }
                float iq = 1.0f / q;
                for (float z : zmap) {
                    if (Math.abs((Math.round((z - min) * iq) * q) + min - z) > E)
                        break quantize;
                }
                if (Math.round((max - min) * iq) > 0xffff) {
                    break quantize;
                } else if (Math.round((max - min) * iq) > 0xff) {
                    fp.adduint8(2).addfloat32(min).addfloat32(q);
                    for (float z : zmap)
                        fp.adduint16(Math.round((z - min) * iq));
                } else {
                    fp.adduint8(1).addfloat32(min).addfloat32(q);
                    for (float z : zmap)
                        fp.adduint8(Math.round((z - min) * iq));
                }
                return;
            }
            fp.adduint8(3);
            for (float z : zmap)
                fp.addfloat32(z);
        }

        public static float[] loadz(Message fp, String nm) {
            float[] ret = new float[cmaps.x * cmaps.y];
            int fmt = fp.uint8();
            if (fmt == 0) {
                float z = fp.float32();
                for (int i = 0; i < ret.length; i++)
                    ret[i] = z;
            } else if (fmt == 1) {
                float min = fp.float32(), q = fp.float32();
                for (int i = 0; i < ret.length; i++)
                    ret[i] = min + (fp.uint8() * q);
            } else if (fmt == 2) {
                float min = fp.float32(), q = fp.float32();
                for (int i = 0; i < ret.length; i++)
                    ret[i] = min + (fp.uint16() * q);
            } else if (fmt == 3) {
                for (int i = 0; i < ret.length; i++)
                    ret[i] = fp.float32();
            } else {
                throw (new Message.FormatError(String.format("Unknown grid z-map format for %s: %d", nm, fmt)));
            }
            return (ret);
        }

        public static void saveols(Message fp, Collection<Overlay> ols) {
            for (Overlay ol : ols) {
                fp.addstring(ol.olid.name);
                fp.adduint16(ol.olid.ver);
                for (int i = 0; i < ol.ol.length; i += 8) {
                    int b = 0;
                    for (int o = 0; o < Math.min(8, ol.ol.length - i); o++) {
                        if (ol.ol[i + o])
                            b |= 1 << o;
                    }
                    fp.adduint8(b);
                }
            }
            fp.addstring("");
        }

        public static void loadols(Collection<Overlay> buf, Message fp, String nm) {
            while (true) {
                String resnm = fp.string();
                if (resnm.equals(""))
                    break;
                int resver = fp.uint16();
                boolean[] ol = new boolean[cmaps.x * cmaps.y];
                for (int i = 0, p = 0; i < ol.length; i += 8) {
                    p = fp.uint8();
                    for (int o = 0; o < Math.min(8, ol.length - i); o++) {
                        if ((p & (1 << o)) != 0)
                            ol[i + o] = true;
                    }
                }
                buf.add(new Overlay(new Resource.Spec(Resource.remote(), resnm, resver), ol));
            }
        }

        public static final Resource.Spec notile = new Resource.Spec(Resource.remote(), "gfx/tiles/notile", -1);
        public static final DataGrid nogrid;

        static {
            nogrid = new DataGrid(new TileInfo[]{new TileInfo(notile, 0)}, new int[cmaps.x * cmaps.y], new float[cmaps.x * cmaps.y], 0);
        }

        private static final Coord[] cset = {Coord.of(-1, -1), Coord.of(0, -1), Coord.of(1, -1), Coord.of(-1, 0), Coord.of(1, 0), Coord.of(-1, 1), Coord.of(0, 1), Coord.of(1, 1)};

        @Deprecated
        private static final Coord[] tecs = {
                new Coord(0, -1),
                new Coord(1, 0),
                new Coord(0, 1),
                new Coord(-1, 0)
        };
        @Deprecated
        private static final Coord[] tccs = {
                new Coord(0, 0),
                new Coord(1, 0),
                new Coord(1, 1),
                new Coord(0, 1)
        };


        @Deprecated
        public boolean isContains(int t, String type) {
            return tilesets[t].res.name().contains(type);
        }

        @Deprecated
        public String tileName(int t) {
            return tilesets[t].res.name;
        }

        @Deprecated
        public float getz(Coord c) {
            return (zmap[c.x + (c.y * cmaps.x)]);
        }

        @Deprecated
        private Tiler tiler(int t, Tiler[] tilers, boolean[] cached) {
            if (cached[t])
                return tilers[t];
            else {
                Resource r = loadsaved(Resource.remote(), tilesets[t].res);
                final Tileset ts = r.layer(Tileset.class);
                if (ts != null) {
                    //This can be null because some tiles are `notile`...
                    final Tiler tile = ts.tfac().create(t, ts);
                    tilers[t] = tile;
                }
                cached[t] = true;
                return tilers[t];
            }
        }

        @Deprecated
        private boolean brokenp(Tiler t, Coord tc, final Tiler[] tilers, final boolean[] tlcache) {
            double bz = ((Ridges.RidgeTile) t).breakz() + Ridges.EPSILON;  //The distance at which a ridge is formed
            //Look at the four tiles around us to get the minimum break distance
            for (Coord ec : tecs) {
                Coord coord = tc.add(ec);
                if (coord.x < 0 || coord.x > cmaps.x - 1 || coord.y < 0 || coord.y > cmaps.y - 1)
                    continue;
                t = tiler(gettile(coord), tilers, tlcache);
                if (t instanceof Ridges.RidgeTile)
                    bz = Math.min(bz, ((Ridges.RidgeTile) t).breakz() + Ridges.EPSILON);
            }

            //Now figure out based on other tiles around us if we hit that break limit and should be a ridge
            //Ignore NOZ heights as these are nonupdated maps
            for (int i = 0; i < tccs.length; i++) {
                Coord coord1 = tc.add(tccs[i]);
                Coord coord2 = tc.add(tccs[(i + 1) % tccs.length]);
                if ((coord1.x < 0 || coord1.x > cmaps.x - 1 || coord1.y < 0 || coord1.y > cmaps.y - 1) ||
                        (coord2.x < 0 || coord2.x > cmaps.x - 1 || coord2.y < 0 || coord2.y > cmaps.y - 1))
                    continue;
                final double z1 = getfz(coord1);
                final double z2 = getfz(coord2);
                //dumb mistake - 99999999
                if (z1 != Double.POSITIVE_INFINITY && z2 != Double.POSITIVE_INFINITY && z1 != Double.NEGATIVE_INFINITY && z2 != Double.NEGATIVE_INFINITY) {
                    if (Math.abs(z2 - z1) > bz) {
                        return (true);
                    }
                }
            }
            return (false);
        }

        @Deprecated
        @SuppressWarnings("Duplicates")
        private static Color simple_tile_img(BufferedImage img) {
            return simple_textures.computeIfAbsent(img, i -> {
                int sumr = 0, sumg = 0, sumb = 0;
                for (int x = 0; x < img.getWidth(); x++) {
                    for (int y = 0; y < img.getHeight(); y++) {
                        int rgb = img.getRGB(x, y);

                        int red = (rgb >> 16) & 0xFF;
                        int green = (rgb >> 8) & 0xFF;
                        int blue = rgb & 0xFF;

                        sumr += red;
                        sumg += green;
                        sumb += blue;
                    }
                }

                int num = img.getWidth() * img.getHeight();
                return new Color(sumr / num, sumg / num, sumb / num);
            });
        }
    }

    public static class Grid extends DataGrid {
        public final long id;
        private boolean[] norepl;
        private int useq = -1;

        public Grid(long id, TileInfo[] tilesets, int[] tiles, float[] zmap, long mtime) {
            super(tilesets, tiles, zmap, mtime);
            this.id = id;
        }

        public static Grid from(MCache map, MCache.Grid cg) {
            int oseq = cg.seq;
            int nt = 0;
            int[] tmap = new int[16];
            int[] rmap = new int[16];
            Arrays.fill(tmap, -1);
            for (int tn : cg.tiles) {
                if (tn >= tmap.length) {
                    int pl = tmap.length;
                    tmap = Utils.extend(tmap, Integer.highestOneBit(tn) * 2);
                    Arrays.fill(tmap, pl, tmap.length, -1);
                }
                if (tmap[tn] == -1) {
                    tmap[tn] = nt;
                    if (nt >= rmap.length)
                        rmap = Utils.extend(rmap, rmap.length * 2);
                    rmap[nt++] = tn;
                }
            }
            TileInfo[] infos = new TileInfo[nt];
            boolean[] norepl = new boolean[nt];
            int[] prios = new int[nt];
            //FIXME from
            //===========
            for (int i = 0, tn = 0; i < tmap.length; i++) {
                if (tmap[i] != -1)
                    prios[tmap[i]] = tn++;
            }
            //===========
            for (int i = 0; i < nt; i++) {
                int tn = rmap[i];
                infos[i] = new TileInfo(map.tilesetn(tn), prios[i]);
                norepl[i] = (Utils.index(Loading.waitfor(() -> map.tileset(tn)).tags, "norepl") >= 0);
            }
            //FIXME to ===========
            int[] tiles = new int[cmaps.x * cmaps.y];
            float[] zmap = new float[cmaps.x * cmaps.y];
            for (int i = 0; i < cg.tiles.length; i++) {
                tiles[i] = tmap[cg.tiles[i]];
                zmap[i] = cg.z[i];
            }
            Grid g = new Grid(cg.id, infos, tiles, zmap, System.currentTimeMillis());
            for (int i = 0; i < cg.ols.length; i++) {
                if (cg.ol[i].length != (cmaps.x * cmaps.y))
                    throw (new AssertionError(String.valueOf(cg.ol[i].length)));
                Resource olres = Loading.waitfor(cg.ols[i]);
                g.ols.add(new Overlay(new Resource.Spec(olres.pool, olres.name, olres.ver), Arrays.copyOf(cg.ol[i], cg.ol[i].length)));
            }
            g.norepl = norepl;
            g.useq = oseq;
            return (g);
        }

        public Grid mergeprev(Grid prev) {
            if ((norepl == null) || (prev.tiles.length != this.tiles.length))
                return (this);
            boolean[] used = new boolean[prev.tilesets.length];
            boolean any = false;
            int[] tmap = new int[prev.tilesets.length];
            for (int i = 0; i < tmap.length; i++)
                tmap[i] = -1;
            for (int i = 0; i < this.tiles.length; i++) {
                if (norepl[this.tiles[i]]) {
                    used[prev.tiles[i]] = true;
                    any = true;
                }
            }
            if (!any)
                return (this);
            TileInfo[] ntilesets = this.tilesets;
            for (int i = 0; i < used.length; i++) {
                if (used[i] && (tmap[i] < 0)) {
                    dedup:
                    {
                        for (int o = 0; o < this.tilesets.length; o++) {
                            if (this.tilesets[o].res.name.equals(prev.tilesets[i].res.name)) {
                                tmap[i] = o;
                                break dedup;
                            }
                        }
                        tmap[i] = ntilesets.length;
                        ntilesets = Utils.extend(ntilesets, prev.tilesets[i]);
                    }
                }
            }
            int[] ntiles = new int[this.tiles.length];
            for (int i = 0; i < this.tiles.length; i++) {
                if (norepl[this.tiles[i]])
                    ntiles[i] = tmap[prev.tiles[i]];
                else
                    ntiles[i] = this.tiles[i];
            }
            Grid g = new Grid(this.id, ntilesets, ntiles, this.zmap, this.mtime);
            g.ols.addAll(this.ols);
            g.useq = this.useq;
            return (g);
        }

        public void save(Message fp) {
            fp.adduint8(5);
            ZMessage z = new ZMessage(fp);
            z.addint64(id);
            z.addint64(mtime);
            savetiles(z, tilesets, tiles);
            savez(z, zmap);
            saveols(z, ols);
            z.finish();
        }

        public void save(MapFile file) {
            OutputStream fp;
            try {
                fp = file.sstore("grid-%x", id);
            } catch (IOException e) {
                throw (new StreamMessage.IOError(e));
            }
            try (StreamMessage out = new StreamMessage(fp)) {
                save(out);
            }
        }

        public static Grid load(MapFile file, long id) {
            InputStream fp;
            try {
                fp = file.sfetch("grid-%x", id);
            } catch (IOException e) {
                warn(e, "error when locating grid %x: %s", id, e);
                return (null);
            }
            try (StreamMessage data = new StreamMessage(fp)) {
                int ver = data.uint8();
                if ((ver >= 1) && (ver <= 5)) {
                    ZMessage z = new ZMessage(data);
                    long storedid = z.int64();
                    if (storedid != id)
                        throw (new Message.FormatError(String.format("Grid ID mismatch: expected %s, got %s", id, storedid)));
                    long mtime = (ver >= 2) ? z.int64() : System.currentTimeMillis();
                    Pair<TileInfo[], int[]> tiles = loadtiles(z, (ver >= 5) ? 2 : 1);
                    float[] zmap;
                    if (ver >= 3)
                        zmap = loadz(z, String.format("%x", id));
                    else
                        zmap = new float[cmaps.x * cmaps.y];
                    Grid g = new Grid(id, tiles.a, tiles.b, zmap, mtime);
                    if (ver >= 4)
                        loadols(g.ols, z, String.format("%x", id));
                    return (g);
                } else {
                    throw (new Message.FormatError(String.format("Unknown grid data version for %x: %d", id, ver)));
                }
            } catch (Message.BinError e) {
                warn(e, "error when loading grid %x: %s", id, e);
                return (null);
            }
        }
    }

    public static class ZoomGrid extends DataGrid {
        public final long seg;
        public final int lvl;
        public final Coord sc;

        public ZoomGrid(long seg, int lvl, Coord sc, TileInfo[] tilesets, int[] tiles, float[] zmap, long mtime) {
            super(tilesets, tiles, zmap, mtime);
            this.seg = seg;
            this.lvl = lvl;
            this.sc = sc;
        }

        /**
         * Get the max mtime of the grids that make up this zoomgrid
         */
        public static long localmtime(MapFile file, Segment seg, int lvl, Coord sc) {
            if ((lvl < 1) || ((sc.x & ((1 << lvl) - 1)) != 0) || ((sc.y & ((1 << lvl) - 1)) != 0))
                throw (new IllegalArgumentException(String.format("%s %s", sc, lvl)));
            DataGrid[] lower = new DataGrid[4];
            long maxmtime = 0;
            for (int i = 0; i < 4; i++) {
                int x = i % 2, y = i / 2;
                lower[i] = fetchg(file, seg, lvl - 1, sc.add(x << (lvl - 1), y << (lvl - 1)));
                if (lower[i] != null) {
                    maxmtime = Math.max(maxmtime, lower[i].mtime);
                }
            }
            return maxmtime;
        }

        public static ZoomGrid fetch(MapFile file, Segment seg, int lvl, Coord sc) {
            ZoomGrid loaded = load(file, seg.id, lvl, sc);
            //zoom grids should update anytime a grid they are made from updated
            if (loaded != null) //FIXME  && loaded.mtime >= localmtime(file, seg, lvl, sc)
                return (loaded);
            return (from(file, seg, lvl, sc));
        }

        private static DataGrid fetchg(MapFile file, Segment seg, int lvl, Coord sc) {
            if (lvl == 0) {
                Long id = seg.map.get(sc);
                if (id == null)
                    return (null);
                return (Grid.load(file, id));
            } else {
                return (fetch(file, seg, lvl, sc));
            }
        }

        public static ZoomGrid from(MapFile file, Segment seg, int lvl, Coord sc) {
            if ((lvl < 1) || ((sc.x & ((1 << lvl) - 1)) != 0) || ((sc.y & ((1 << lvl) - 1)) != 0))
                throw (new IllegalArgumentException(String.format("%s %s", sc, lvl)));
            DataGrid[] lower = new DataGrid[4];
            boolean any = false;
            long maxmtime = 0;
            for (int i = 0; i < 4; i++) {
                int x = i % 2, y = i / 2;
                lower[i] = fetchg(file, seg, lvl - 1, sc.add(x << (lvl - 1), y << (lvl - 1)));
                if (lower[i] != null) {
                    any = true;
                    maxmtime = Math.max(maxmtime, lower[i].mtime);
                } else {
                    lower[i] = DataGrid.nogrid;
                }
            }
            if (!any)
                return (null);

            /* XXX: This is hardly "correct", but the correct
             * implementation would require a topological sort, and
             * it's not like it really matters that much. */
            int nt = 0;
            TileInfo[] infos;
            Map<String, Integer> rinfos;
            {
                Resource.Pool pool = null;
                String[] sets = new String[16];
                Set<String> hassets = new HashSet<>();
                Map<String, Integer> vers = new HashMap<>();
                for (int i = 0; i < 4; i++) {
                    if (lower[i] == null)
                        continue;
                    for (int tn = 0; tn < lower[i].tilesets.length; tn++) {
                        Resource.Spec set = lower[i].tilesets[tn].res;
                        if (pool == null)
                            pool = set.pool;
                        vers.put(set.name, Math.max(vers.getOrDefault(set.name, 0), set.ver));
                        if (!hassets.contains(set.name)) {
                            while (nt >= sets.length)
                                sets = Utils.extend(sets, sets.length * 2);
                            sets[nt++] = set.name;
                            hassets.add(set.name);
                        }
                    }
                }
                infos = new TileInfo[nt];
                rinfos = new HashMap<>();
                for (int i = 0; i < nt; i++) {
                    infos[i] = new TileInfo(new Resource.Spec(pool, sets[i], vers.get(sets[i])), i);
                    rinfos.put(sets[i], i);
                }
            }

            int[] tiles = new int[cmaps.x * cmaps.y];
            float[] zmap = new float[cmaps.x * cmaps.y];
            for (int gn = 0; gn < 4; gn++) {
                int gx = gn % 2, gy = gn / 2;
                DataGrid cg = lower[gn];
                if (cg == null)
                    continue;
                int[] tmap = new int[cg.tilesets.length];
                Arrays.fill(tmap, (byte) -1);
                for (int i = 0; i < cg.tilesets.length; i++)
                    tmap[i] = rinfos.get(cg.tilesets[i].res.name);
                Coord off = cmaps.div(2).mul(gx, gy);
                int[] tc = new int[4];
                byte[] tcn = new byte[4];
                for (int y = 0; y < cmaps.y / 2; y++) {
                    for (int x = 0; x < cmaps.x / 2; x++) {
                        int nd = 0;
                        float minz = Float.POSITIVE_INFINITY;
                        for (int sy = 0; sy < 2; sy++) {
                            for (int sx = 0; sx < 2; sx++) {
                                Coord sgc = new Coord((x * 2) + sx, (y * 2) + sy);
                                int st = tmap[cg.gettile(sgc)];
                                minz = Math.min(minz, (float) cg.getfz(sgc));
                                st:
                                {
                                    for (int i = 0; i < nd; i++) {
                                        if (tc[i] == st) {
                                            tcn[i]++;
                                            break st;
                                        }
                                    }
                                    tc[nd] = st;
                                    tcn[nd] = 1;
                                    nd++;
                                }
                            }
                        }
                        int mi = 0;
                        for (int i = 1; i < nd; i++) {
                            if (tcn[i] > tcn[mi])
                                mi = i;
                        }
                        tiles[(x + off.x) + ((y + off.y) * cmaps.x)] = tc[mi];
                        zmap[(x + off.x) + ((y + off.y) * cmaps.x)] = minz;
                    }
                }
            }
            ZoomGrid ret = new ZoomGrid(seg.id, lvl, sc, infos, tiles, zmap, maxmtime);
            zoomols(ret.ols, lower);
            ret.save(file);
            return (ret);
        }

        private static void zoomols(Collection<Overlay> buf, DataGrid[] lower) {
            for (int gn = 0; gn < 4; gn++) {
                int gx = gn % 2, gy = gn / 2;
                DataGrid cg = lower[gn];
                if (cg == null)
                    continue;
                Coord off = cmaps.div(2).mul(gx, gy);
                for (Overlay ol : cg.ols) {
                    Overlay zol = null;
                    for (Overlay pol : buf) {
                        if (pol.olid.name.equals(ol.olid.name)) {
                            zol = pol;
                            break;
                        }
                    }
                    for (int y = 0; y < cmaps.y / 2; y++) {
                        for (int x = 0; x < cmaps.x / 2; x++) {
                            int n = 0;
                            for (int sy = 0; sy < 2; sy++) {
                                for (int sx = 0; sx < 2; sx++) {
                                    Coord sgc = new Coord((x * 2) + sx, (y * 2) + sy);
                                    if (ol.get(sgc))
                                        n++;
                                }
                            }
                            if (n >= 2) {
                                if (zol == null)
                                    buf.add(zol = new Overlay(ol.olid, new boolean[cmaps.x * cmaps.y]));
                                zol.ol[(x + off.x) + ((y + off.y) * cmaps.x)] = true;
                            }
                        }
                    }
                }
            }
        }

        public void save(Message fp) {
            fp.adduint8(4);
            ZMessage z = new ZMessage(fp);
            z.addint64(seg);
            z.addint32(lvl);
            z.addcoord(sc);
            z.addint64(mtime);
            savetiles(z, tilesets, tiles);
            savez(z, zmap);
            saveols(z, ols);
            z.finish();
        }

        public void save(MapFile file) {
            OutputStream fp;
            try {
                fp = file.sstore("zgrid-%x-%d-%d-%d", seg, lvl, sc.x, sc.y);
            } catch (IOException e) {
                throw (new StreamMessage.IOError(e));
            }
            try (StreamMessage out = new StreamMessage(fp)) {
                save(out);
            }
        }

        public static ZoomGrid load(MapFile file, long seg, int lvl, Coord sc) {
            InputStream fp;
            try {
                fp = file.sfetch("zgrid-%x-%d-%d-%d", seg, lvl, sc.x, sc.y);
            } catch (FileNotFoundException e) {
                return (null);
            } catch (IOException e) {
                warn(e, "error when locating zoomgrid (%d, %d) in %x@%d: %s", sc.x, sc.y, seg, lvl, e);
                return (null);
            }
            try (StreamMessage data = new StreamMessage(fp)) {
                if (data.eom())
                    return (null);
                int ver = data.uint8();
                if ((ver >= 1) && (ver <= 4)) {
                    ZMessage z = new ZMessage(data);
                    long storedseg = z.int64();
                    if (storedseg != seg)
                        throw (new Message.FormatError(String.format("Zoomgrid segment mismatch: expected %s, got %s", seg, storedseg)));
                    long storedlvl = z.int32();
                    if (storedlvl != lvl)
                        throw (new Message.FormatError(String.format("Zoomgrid level mismatch: expected %s, got %s", lvl, storedlvl)));
                    Coord storedsc = z.coord();
                    if (!sc.equals(storedsc))
                        throw (new Message.FormatError(String.format("Zoomgrid coord mismatch: expected %s, got %s", sc, storedsc)));

                    long mtime = z.int64();
                    Pair<TileInfo[], int[]> tiles = loadtiles(z, (ver >= 4) ? 2 : 1);
                    float[] zmap;
                    if (ver >= 2)
                        zmap = loadz(z, String.format("(%d, %d) in %x@d", sc.x, sc.y, seg, lvl));
                    else
                        zmap = new float[cmaps.x * cmaps.y];
                    ZoomGrid g = new ZoomGrid(seg, lvl, sc, tiles.a, tiles.b, zmap, mtime);
                    if (ver >= 3)
                        loadols(g.ols, z, String.format("(%d, %d) in %x@d", sc.x, sc.y, seg, lvl));
                    return (g);
                } else {
                    throw (new Message.FormatError(String.format("Unknown zoomgrid data version for (%d, %d) in %x@%d: %d", sc.x, sc.y, seg, lvl, ver)));
                }
            } catch (Message.BinError e) {
                warn(e, "could not load zoomgrid for (%d, %d) in %x@%d: %s", sc.x, sc.y, seg, lvl, e);
                return (null);
            }
        }

        public static int inval(MapFile file, long seg, Coord sc) {
            for (int lvl = 1; true; lvl++) {
                sc = new Coord(sc.x & ~((1 << lvl) - 1), sc.y & ~((1 << lvl) - 1));
                try {
                    file.sfetch("zgrid-%x-%d-%d-%d", seg, lvl, sc.x, sc.y).close();
                } catch (FileNotFoundException e) {
                    return (lvl - 1);
                } catch (IOException e) {
                    warn(e, "error when invalidating zoomgrid (%d, %d) in %x@%d: %s", sc.x, sc.y, seg, lvl, e);
                    return (lvl - 1);
                }
                try {
                    file.sstore("zgrid-%x-%d-%d-%d", seg, lvl, sc.x, sc.y).close();
                } catch (IOException e) {
                    throw (new StreamMessage.IOError(e));
                }
            }
        }
    }

    public static class ZoomCoord {
        public final int lvl;
        public final Coord c;

        public ZoomCoord(int lvl, Coord c) {
            this.lvl = lvl;
            this.c = c;
        }

        public int hashCode() {
            return ((c.hashCode() * 31) + lvl);
        }

        public boolean equals(Object o) {
            if (!(o instanceof ZoomCoord))
                return (false);
            ZoomCoord that = (ZoomCoord) o;
            return ((this.lvl == that.lvl) && this.c.equals(that.c));
        }

        public String toString() {
            return (String.format("(%d, %d @ %d)", c.x, c.y, lvl));
        }
    }

    public class Segment {
        public final long id;
        public final BMap<Coord, Long> map = new HashBMap<>();
        private final Map<Long, Cached> cache = new CacheMap<>(CacheMap.RefType.WEAK);
        private final Map<Coord, ByCoord> ccache = new CacheMap<>(CacheMap.RefType.WEAK);
        private final Map<ZoomCoord, ByZCoord> zcache = new CacheMap<>(CacheMap.RefType.WEAK);

        public Segment(long id) {
            this.id = id;
        }

        public MapFile file() {
            return (MapFile.this);
        }

        private class Cached implements Indir<Grid> {
            Grid loaded;
            Future<Grid> loading;

            Cached(Future<Grid> loading) {
                this.loading = loading;
            }

            public Grid get() {
                if (loaded == null)
                    loaded = loading.get(0);
                return (loaded);
            }
        }

        private Grid loaded(long id) {
            checklock();
            synchronized (cache) {
                Cached cur = cache.get(id);
                if (cur != null)
                    return (cur.loaded);
            }
            return (null);
        }

        private Future<Grid> loadgrid(long id) {
            return (Defer.later(() -> Grid.load(MapFile.this, id)));
        }

        private Cached grid0(long id) {
            checklock();
            synchronized (cache) {
                return (cache.computeIfAbsent(id, k -> new Cached(loadgrid(k))));
            }
        }

        public Indir<Grid> grid(long id) {
            return (grid0(id));
        }

        private class ByCoord implements Indir<Grid> {
            final Coord sc;
            Cached cur;

            ByCoord(Coord sc, Cached cur) {
                this.sc = sc;
                this.cur = cur;
            }

            public Grid get() {
                Cached cur = this.cur;
                if (cur == null)
                    return (null);
                return (cur.get());
            }
        }

        private Future<ZoomGrid> loadzgrid(ZoomCoord zc) {
            return (Defer.later(() -> ZoomGrid.fetch(MapFile.this, Segment.this, zc.lvl, zc.c)));
        }

        private class ByZCoord implements Indir<ZoomGrid> {
            final ZoomCoord zc;
            ZoomGrid loaded;
            boolean got = false;
            Future<ZoomGrid> loading;

            ByZCoord(ZoomCoord zc, Future<ZoomGrid> loading) {
                this.zc = zc;
                this.loading = loading;
            }

            public ZoomGrid get() {
                if (loading != null) {
                    try {
                        loaded = loading.get(0);
                        got = true;
                        loading = null;
                    } catch (Loading l) {
                        if (!got)
                            throw (l);
                    }
                }
                return (loaded);
            }
        }

        public Indir<Grid> grid(Coord gc) {
            checklock();
            synchronized (ccache) {
                return (ccache.computeIfAbsent(gc, k -> {
                    Long id = map.get(k);
                    Cached cur = (id == null) ? null : grid0(id);
                    return (new ByCoord(k, cur));
                }));
            }
        }

        public Indir<? extends DataGrid> grid(int lvl, Coord gc) {
            if ((lvl < 0) || ((gc.x & ((1 << lvl) - 1)) != 0) || ((gc.y & ((1 << lvl) - 1)) != 0))
                throw (new IllegalArgumentException(String.format("%s %s", gc, lvl)));
            if (lvl == 0)
                return (grid(gc));
            synchronized (zcache) {
                ZoomCoord zc = new ZoomCoord(lvl, gc);
                return (zcache.computeIfAbsent(zc, k -> new ByZCoord(k, loadzgrid(k))));
            }
        }

        private void include(long id, Coord sc) {
            map.put(sc, id);
//            int zl = ZoomGrid.inval(MapFile.this, this.id, sc);
            synchronized (zcache) {
                /* XXX? Not sure how nice it is to iterate through the
                 * entire zcache to do invalidations, but I also don't
                 * think it ought to tend to be enormously large.
                 * Perhaps keep a hierarchical zcache per level, and
                 * only iterate all the levels? */
                for (Map.Entry<ZoomCoord, ByZCoord> ent : zcache.entrySet()) {
                    ZoomCoord zc = ent.getKey();
                    if ((zc.c.x == (sc.x & ~((1 << zc.lvl) - 1))) && (zc.c.y == (sc.y & ~((1 << zc.lvl) - 1)))) {
                        ByZCoord zg = ent.getValue();
                        zg.loading = loadzgrid(zc);
                    }
                }
            }
            ByCoord bc;
            synchronized (ccache) {
                bc = ccache.get(sc);
            }
            if ((bc != null) && (bc.cur == null))
                bc.cur = grid0(id);
        }

        private void include(Grid grid, Coord sc) {
            checklock();
            include(grid.id, sc);
            synchronized (cache) {
                Cached cur = cache.get(grid.id);
                if (cur != null)
                    cur.loaded = grid;
            }
        }

        @Deprecated
        public String gridtilename(final Coord tc, final Coord gc) {
            if (map.containsKey(gc)) {
                if (cache.containsKey(map.get(gc))) {
                    final Grid g = cache.get(map.get(gc)).loaded;
                    return g.tilesets[g.gettile(tc.sub(gc.mul(cmaps)))].res.name;
                } else {
                    return "Unknown";
                }
            } else {
                return "Unknown";
            }
        }

        @Deprecated
        public long gridid(Coord gc) {
            return map.get(gc);
        }

        @Deprecated
        public int gridseq(Coord gc) {
            if (map.containsKey(gc)) {
                return cache.get(map.get(gc)).loaded.useq;
            } else {
                return -10;
            }
        }

        @Deprecated
        public void remove(Coord coord) {
            Long gridid = map.get(coord);
            if (gridid != null) {
                map.remove(coord);
                cache.remove(gridid);
                ccache.remove(coord);
                int zl = ZoomGrid.inval(MapFile.this, this.id, coord);
                synchronized (zcache) {
                    for (int lvl = 1; lvl < zl; lvl++) {
                        ZoomCoord zc = new ZoomCoord(lvl, new Coord(coord.x & ~((1 << lvl) - 1), coord.y & ~((1 << lvl) - 1)));
                        zcache.remove(zc);
                    }
                }
            }
        }
    }

    public static class View implements MapSource {
        public final Segment seg;
        private final Map<Coord, GridMap> grids = new HashMap<>();
        private Resource.Spec[] nsets;
        private Tileset[] tilesets;
        private Tiler[] tiles;

        public View(Segment seg) {
            this.seg = seg;
        }

        private class GridMap {
            final Grid grid;
            final Coord gc;
            int[] tilemap = null;

            GridMap(Grid grid, Coord gc) {
                this.grid = grid;
                this.gc = gc;
            }
        }

        public void addgrid(Coord gc) {
            if (!grids.containsKey(gc)) {
                Grid grid = seg.grid(gc).get();
                if (grid == null)
                    grids.put(gc, null);
                else
                    grids.put(gc, new GridMap(grid, gc));
            }
        }

        private static class TileSort extends TopoSort<String> {
            TileSort() {
                super(Hash.eq);
            }

            protected List<String> pick(Collection<String> from) {
                List<String> ret = new ArrayList<>(from);
                Collections.sort(ret);
                return (ret);
            }

            protected List<String> pickbad() {
                Collection<Collection<String>> cycles = findcycles();
                // System.err.println("inconsistent tile ordering found: " + cycles);
                List<String> ret = new ArrayList<>(Utils.el(cycles));
                Collections.sort(ret);
                return (ret);
            }
        }

        public void fin() {
            Map<String, Resource.Spec> vermap = new HashMap<>();
            TopoSort<String> tilesort = new TileSort();
            for (GridMap gm : grids.values()) {
                if (gm == null)
                    continue;
                Grid g = gm.grid;
                Collection<String> order = new ArrayList<>();
                List<TileInfo> gtiles = new ArrayList<>(Arrays.asList(g.tilesets));
                Collections.sort(gtiles, (a, b) -> (a.prio - b.prio));
                for (TileInfo tinf : gtiles) {
                    if (!vermap.containsKey(tinf.res.name) || (vermap.get(tinf.res.name).ver < tinf.res.ver))
                        vermap.put(tinf.res.name, tinf.res);
                    order.add(tinf.res.name);
                }
                tilesort.add(order);
            }
            String[] ordered = tilesort.sort().toArray(new String[0]);
            Resource.Spec[] nsets = new Resource.Spec[ordered.length];
            for (int i = 0; i < ordered.length; i++)
                nsets[i] = vermap.get(ordered[i]);
            Map<String, Integer> idx = new HashMap<>();
            for (int i = 0; i < nsets.length; i++)
                idx.put(nsets[i].name, i);
            for (GridMap gm : grids.values()) {
                if (gm == null)
                    continue;
                int[] xl = new int[gm.grid.tilesets.length];
                for (int i = 0; i < xl.length; i++)
                    xl[i] = idx.get(gm.grid.tilesets[i].res.name);
                gm.tilemap = xl;
            }
            this.nsets = nsets;
            this.tilesets = new Tileset[nsets.length];
            this.tiles = new Tiler[nsets.length];
        }

        private Coord cachedgc = null;
        private GridMap cached = null;

        private GridMap getgrid(Coord gc) {
            if ((cachedgc == null) || !cachedgc.equals(gc)) {
                cached = grids.get(gc);
                cachedgc = gc;
            }
            return (cached);
        }

        public int gettile(Coord tc) {
            Coord gc = tc.div(cmaps);
            Coord ul = gc.mul(cmaps);
            GridMap gm = getgrid(gc);
            if (gm == null)
                return (-1);
            if (gm.tilemap == null)
                throw (new IllegalStateException("Not finalized"));
            return (gm.tilemap[gm.grid.gettile(tc.sub(ul))]);
        }

        public double getfz(Coord tc) {
            Coord gc = tc.div(cmaps);
            Coord ul = gc.mul(cmaps);
            GridMap gm = getgrid(gc);
            if (gm == null)
                return (0);
            return (gm.grid.getfz(tc.sub(ul)));
        }

        public Tileset tileset(int n) {
            if (tilesets[n] == null) {
                Resource res = nsets[n].loadsaved(Resource.remote());
                tilesets[n] = res.flayer(Tileset.class);
            }
            return (tilesets[n]);
        }

        public Tiler tiler(int n) {
            if (tiles[n] == null) {
                Tileset set = tileset(n);
                tiles[n] = set.tfac().create(n, set);
            }
            return (tiles[n]);
        }
    }

    public final BackCache<Long, Segment> segments = new BackCache<>(5, id -> {
        checklock();
        InputStream fp;
        try {
            fp = sfetch("seg-%x", id);
        } catch (IOException e) {
            return (null);
        }
        try (StreamMessage data = new StreamMessage(fp)) {
            if (data.eom())
                return (null);
            int ver = data.uint8();
            if (ver == 1) {
                Segment seg = new Segment(id);
                ZMessage z = new ZMessage(data);
                long storedid = z.int64();
                if (storedid != id)
                    throw (new Message.FormatError(String.format("Segment ID mismatch: expected %x, got %x", id, storedid)));
                for (int i = 0, no = z.int32(); i < no; i++)
                    seg.map.put(z.coord(), z.int64());
                return (seg);
            } else {
                throw (new Message.FormatError("Unknown segment data version: " + ver));
            }
        } catch (Message.BinError e) {
            warn(e, "error when loading segment %x: %s", id, e);
            return (null);
        }
    }, (id, seg) -> {
        checklock();
        if (seg == null) {
            try (OutputStream fp = sstore("seg-%x", id)) {
            } catch (IOException e) {
                throw (new StreamMessage.IOError(e));
            }
            if (knownsegs.remove(id))
                defersave();
        } else {
            OutputStream fp;
            try {
                fp = sstore("seg-%x", seg.id);
            } catch (IOException e) {
                throw (new StreamMessage.IOError(e));
            }
            try (StreamMessage out = new StreamMessage(fp)) {
                out.adduint8(1);
                ZMessage z = new ZMessage(out);
                z.addint64(seg.id);
                z.addint32(seg.map.size());
                for (Map.Entry<Coord, Long> e : seg.map.entrySet())
                    z.addcoord(e.getKey()).addint64(e.getValue());
                z.finish();
            }
            if (knownsegs.add(id))
                defersave();
        }
    });

    private void merge(Segment dst, Segment src, Coord soff) {
        checklock();
        for (Map.Entry<Coord, Long> gi : src.map.entrySet()) {
            long id = gi.getValue();
            Coord sc = gi.getKey();
            Coord dc = sc.sub(soff);
            dst.include(id, dc);
            gridinfo.put(id, new GridInfo(id, dst.id, dc));
        }
        boolean mf = false;
        for (Marker mark : markers) {
            if (mark.seg == src.id) {
                mark.seg = dst.id;
                mark.tc = mark.tc.sub(soff.mul(cmaps));
                mf = true;
            }
        }
        if (mf)
            markerseq++;
        knownsegs.remove(src.id);
        defersave();
        synchronized (procmon) {
            dirty.add(dst);
            process();
        }
    }

    public void update(MCache map, Collection<MCache.Grid> grids) {
        lock.writeLock().lock();
        try {
            long mseg = -1;
            Coord moff = null;
            Collection<MCache.Grid> missing = new ArrayList<>(grids.size());
            Collection<Pair<Long, Coord>> merge = null;
            for (MCache.Grid g : grids) {
                GridInfo info = gridinfo.get(g.id);
                if (info == null) {
                    missing.add(g);
                    continue;
                }
                Segment seg = segments.get(info.seg);
                if (seg == null) {
                    missing.add(g);
                    continue;
                }
                if (moff == null) {
                    Coord psc = seg.map.reverse().get(g.id);
                    if (psc == null) {
                        warn("grid %x is oddly gone from segment %x; was at %s", g.id, seg.id, info.sc);
                        missing.add(g);
                        continue;
                    } else if (!psc.equals(info.sc)) {
                        warn("segment-offset mismatch for grid %x in segment %x: segment has %s, gridinfo has %s", g.id, seg.id, psc, info.sc);
                        missing.add(g);
                        continue;
                    }
                    mseg = seg.id;
                    moff = info.sc.sub(g.gc);
                }
                Grid cur = seg.loaded(g.id);
                if (!((cur != null) && (cur.useq == g.seq))) {
                    Grid sg = Grid.from(map, g);
                    Grid prev = cur;
                    if (prev == null)
                        prev = Grid.load(MapFile.this, sg.id);
                    if (prev != null)
                        sg = sg.mergeprev(prev);
                    sg.save(MapFile.this);
                    seg.include(sg, info.sc);
                }
                if (seg.id != mseg) {
                    if (merge == null)
                        merge = new HashSet<>();
                    Coord soff = info.sc.sub(g.gc.add(moff));
                    merge.add(new Pair<>(seg.id, soff));
                }
            }
            if (!missing.isEmpty()) {
                Segment seg;
                if (mseg == -1) {
                    seg = new Segment(rnd.nextLong());
                    moff = Coord.z;
                    warn("mapfile: creating new segment %x", seg.id);
                } else {
                    seg = segments.get(mseg);
                }
                synchronized (procmon) {
                    dirty.add(seg);
                    process();
                }
                for (MCache.Grid g : missing) {
                    Grid sg = Grid.from(map, g);
                    Coord sc = g.gc.add(moff);
                    sg.save(MapFile.this);
                    seg.include(sg, sc);
                    gridinfo.put(g.id, new GridInfo(g.id, seg.id, sc));
                }
            }
            if (merge != null) {
                for (Pair<Long, Coord> mel : merge) {
                    Segment a = segments.get(mseg);
                    Segment b = segments.get(mel.a);
                    Coord ab = mel.b;
                    Segment src, dst;
                    Coord soff;
                    if (a.map.size() > b.map.size()) {
                        src = b;
                        dst = a;
                        soff = ab;
                    } else {
                        src = a;
                        dst = b;
                        soff = ab.inv();
                    }
                    warn("mapfile: merging segment %x (%d) into %x (%d) at %s", src.id, src.map.size(), dst.id, dst.map.size(), soff);
                    merge(dst, src, soff);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
        warn("mapfile: update completed");
    }

    public interface ExportFilter {
        boolean includeseg(long id);

        boolean includegrid(Segment seg, Coord sc, long id);

        boolean includemark(Marker mark);

        ExportFilter all = new ExportFilter() {
            public boolean includeseg(long id) {
                return (true);
            }

            public boolean includegrid(Segment seg, Coord sc, long id) {
                return (true);
            }

            public boolean includemark(Marker mark) {
                return (true);
            }
        };

        static ExportFilter segment(long sid) {
            return (new ExportFilter() {
                public boolean includeseg(long id) {
                    return (id == sid);
                }

                public boolean includegrid(Segment seg, Coord sc, long id) {
                    return (seg.id == sid);
                }

                public boolean includemark(Marker mark) {
                    return (mark.seg == sid);
                }
            });
        }

        static ExportFilter around(Marker mark, double rad) {
            return (new ExportFilter() {
                public boolean includeseg(long id) {
                    return (id == mark.seg);
                }

                public boolean includegrid(Segment seg, Coord sc, long id) {
                    return ((seg.id == mark.seg) && (sc.mul(cmaps).add(cmaps.div(2)).dist(mark.tc) <= rad));
                }

                public boolean includemark(Marker cmark) {
                    return (cmark == mark);
                }
            });
        }
    }

    public interface ExportStatus {
        default void grid(int cs, int ns, int cg, int ng) {
        }

        default void info(String text) {
        }

        default void mark(int cm, int nm) {
        }
    }

    private static final byte[] EXPORT_SIG = "Haven Mapfile 1".getBytes(Utils.ascii);

    public void export(boolean errors, int v, Message out, ExportFilter filter, ExportStatus prog) throws InterruptedException {
        if (prog == null) prog = new ExportStatus() {
        };
        out.addbytes(EXPORT_SIG);
        ZMessage zout = new ZMessage(out);
        Collection<Long> segbuf = locked((Collection<Long> c) -> new ArrayList<>(c), lock.readLock()).apply(knownsegs);
        int nseg = 0;
        Collection<Long> ids = new ArrayList<>();
        Collection<Long> dids = new ArrayList<>();
        for (Long sid : segbuf) {
            if (!filter.includeseg(sid))
                continue;
            Segment seg;
            Collection<Pair<Coord, Long>> gridbuf = new ArrayList<>();
            lock.readLock().lock();
            try {
                seg = segments.get(sid);
                for (Map.Entry<Coord, Long> gd : seg.map.entrySet()) {
                    if (filter.includegrid(seg, gd.getKey(), gd.getValue()))
                        gridbuf.add(new Pair<>(gd.getKey(), gd.getValue()));
                    prog.info("Calculating map cut " + gridbuf.size() + "/" + seg.map.size());
                }
            } finally {
                lock.readLock().unlock();
            }
            int ngrid = 0;
            if (!errors) {
                int dgrid = 0;
                for (Pair<Coord, Long> gd : gridbuf) { //check for bugs
                    if (ids.contains(gd.b)) {
                        if (!dids.contains(gd.b))
                            dids.add(gd.b);
                    } else {
                        ids.add(gd.b);
                    }
                    prog.info("Checking map cut " + dgrid++ + "/" + gridbuf.size());
                }
            }
            for (Pair<Coord, Long> gd : gridbuf) {
                prog.grid(nseg, segbuf.size(), ngrid++, gridbuf.size());
                if (!errors && dids.contains(gd.b))
                    continue;
                Grid grid = Grid.load(this, gd.b);
                if (grid == null) {
                    /* This /should/ never happen, but for unknown
                     * reasons (crashes? reboots?) some grids can be
                     * included but missing. It's not like they'll be
                     * coming back by any other means, however, so
                     * just ignore them here. */
                    continue;
                }
                MessageBuf buf = new MessageBuf();
                if (v == -1) {
                    buf.adduint8(4);
                    buf.addint64(gd.b);
                    buf.addint64(seg.id);
                    buf.addint64(grid.mtime);
                    buf.addcoord(gd.a);
                    buf.addint32(cmaps.x * cmaps.y);
                    DataGrid.savetiles(buf, grid.tilesets, grid.tiles);
                    DataGrid.savez(buf, grid.zmap);
                    DataGrid.saveols(buf, grid.ols);
                    byte[] od = buf.fin();
                    zout.addstring("grid");
                    zout.addint32(od.length);
                    zout.addbytes(od);
                } else if (v == 3) {
                    buf.adduint8(3);
                    buf.addint64(gd.b);
                    buf.addint64(seg.id);
                    buf.addint64(grid.mtime);
                    buf.addcoord(gd.a);
                    buf.adduint8(grid.tilesets.length);
                    for (TileInfo tinf : grid.tilesets) {
                        buf.addstring(tinf.res.name);
                        buf.adduint16(tinf.res.ver);
                        buf.adduint8(tinf.prio);
                    }
                    buf.addint32(cmaps.x * cmaps.y);
                    for (int tn : grid.tiles)
                        buf.adduint8(tn);
                    DataGrid.savez(buf, grid.zmap);
                    DataGrid.saveols(buf, grid.ols);
                    byte[] od = buf.fin();
                    zout.addstring("grid");
                    zout.addint32(od.length);
                    zout.addbytes(od);
                }
                Utils.checkirq();
            }
            nseg++;
        }
        Collection<Marker> markbuf = locked((Collection<Marker> c) -> new ArrayList<>(c), lock.readLock()).apply(markers);
        int nmark = 0;
        for (Marker mark : markbuf) {
            prog.mark(nmark++, markbuf.size());
            if (!filter.includemark(mark))
                continue;
            if (mark instanceof SMarker) {
                if (!((SMarker) mark).autosend) continue;
            }
            MessageBuf buf = new MessageBuf();
            savemarker(buf, mark);
            byte[] od = buf.fin();
            zout.addstring("mark");
            zout.addint32(od.length);
            zout.addbytes(od);
            Utils.checkirq();
        }
        zout.finish();
    }

    public void export(boolean errors, int v, OutputStream out, ExportFilter filter, ExportStatus prog) throws InterruptedException {
        StreamMessage msg = new StreamMessage(null, out);
        export(errors, v, msg, filter, prog);
        msg.flush();
    }

    public static class ImportedGrid {
        public long gid, segid, mtime;
        public Coord sc;
        public TileInfo[] tilesets;
        public int[] tiles;
        public float[] zmap;
        public Collection<Overlay> ols = new ArrayList<>();

        ImportedGrid(Message data) {
            int ver = data.uint8();
            if ((ver < 1) || (ver > 4))
                throw (new Message.FormatError("Unknown grid data version: " + ver));
            gid = data.int64();
            segid = data.int64();
            mtime = data.int64();
            sc = data.coord();
            if (ver >= 4) {
                int len = data.int32();
                if (len != (cmaps.x * cmaps.y))
                    throw (new Message.FormatError("Bad grid data dimensions: " + len));
                Pair<TileInfo[], int[]> tileinfo = DataGrid.loadtiles(data, 2);
                tilesets = tileinfo.a;
                tiles = tileinfo.b;
                zmap = DataGrid.loadz(data, String.format("%x", gid));
            } else {
                tilesets = new TileInfo[data.uint8()];
                for (int i = 0; i < tilesets.length; i++)
                    tilesets[i] = new TileInfo(new Resource.Spec(Resource.remote(), data.string(), data.uint16()), data.uint8());
                if (ver >= 2) {
                    int len = data.int32();
                    if (len != (cmaps.x * cmaps.y))
                        throw (new Message.FormatError("Bad grid data dimensions: " + len));
                    tiles = new int[len];
                    for (int i = 0; i < len; i++)
                        tiles[i] = data.uint8();
                    zmap = DataGrid.loadz(data, String.format("%x", gid));
                } else {
                    byte[] raw = data.bytes();
                    if (raw.length != (cmaps.x * cmaps.y))
                        throw (new Message.FormatError("Bad grid data dimensions: " + tiles.length));
                    tiles = new int[raw.length];
                    for (int i = 0; i < raw.length; i++)
                        tiles[i] = raw[i] & 0xff;
                    zmap = new float[cmaps.x * cmaps.y];
                }
            }
            if (ver >= 3)
                DataGrid.loadols(ols, data, String.format("%x", gid));
            for (int td : tiles) {
                if (td >= tiles.length)
                    throw (new Message.FormatError(String.format("Bad grid data contents: Tileset ID %d does not exist among 0-%d", (td & 0xff), tiles.length - 1)));
            }
        }

        Grid togrid() {
            Grid ret = new Grid(gid, tilesets, tiles, zmap, mtime);
            ret.ols.addAll(ols);
            return (ret);
        }
    }

    public interface ImportFilter {
        boolean includegrid(ImportedGrid grid, boolean hasprev);

        boolean includemark(Marker mark, Marker prev);

        default void handleerror(RuntimeException exc, String ctx) {
            throw (exc);
        }

        ImportFilter all = new ImportFilter() {
            public boolean includegrid(ImportedGrid grid, boolean hasprev) {
                return (true);
            }

            public boolean includemark(Marker mark, Marker prev) {
                return (prev == null);
            }
        };

        ImportFilter readonly = new ImportFilter() {
            public boolean includegrid(ImportedGrid grid, boolean hasprev) {
                return (false);
            }

            public boolean includemark(Marker mark, Marker prev) {
                return (false);
            }
        };
    }

    private class Importer {
        final Map<Long, ImportedSegment> segs = new HashMap<>();
        final ImportFilter filter;
        Segment curseg;

        class ImportedSegment {
            final Map<Long, Coord> offs = new HashMap<>();
            long nseg;
            Coord noff = null;
        }

        Importer(ImportFilter filter) {
            this.filter = filter;
        }

        void flush() {
            chseg(null);
        }

        Segment chseg(Segment nseg) {
            if ((curseg != null) && (curseg != nseg)) {
                locked(() -> segments.put(curseg.id, curseg), lock.writeLock()).run();
            }
            return (curseg = nseg);
        }

        Segment chseg(long id) {
            if ((curseg != null) && (curseg.id == id))
                return (curseg);
            Segment ret;
            lock.readLock().lock();
            try {
                ret = segments.get(id);
            } finally {
                lock.readLock().unlock();
            }
            return (chseg(ret));
        }

        void importgrid(boolean errors, Message data) {
            ImportedGrid grid = new ImportedGrid(data);
            ImportedSegment seg = segs.get(grid.segid);
            if (seg == null) {
                segs.put(grid.segid, seg = new ImportedSegment());
            }
            GridInfo info;
            lock.readLock().lock();
            try {
                info = gridinfo.get(grid.gid);
            } finally {
                lock.readLock().unlock();
            }
            if (info != null) {
                Coord off = seg.offs.get(info.seg);
                if (off == null) {
                    seg.offs.put(info.seg, info.sc.sub(grid.sc));
                } else {
                    if (!off.equals(info.sc.sub(grid.sc)) && !errors) {
                        return;
//                        throw (new RuntimeException("Inconsistent grid locations detected"));
                    }
                }
            }
            Segment rseg;
            if (filter.includegrid(grid, info != null)) {
                lock.writeLock().lock();
                try {
                    skip:
                    {
                        Grid rgrid = grid.togrid();
                        rgrid.save(MapFile.this);
                        if (seg.noff == null) {
                            if (info == null) {
                                rseg = chseg(new Segment(seg.nseg = grid.gid));
                                seg.noff = Coord.z;
                                seg.offs.put(seg.nseg, Coord.z);
                            } else {
                                rseg = chseg(seg.nseg = info.seg);
                                if (rseg == null) {
                                    System.out.println("rseg is null " + seg.nseg);
                                    break skip;
                                }
                                seg.noff = seg.offs.get(info.seg);
                            }
                        } else {
                            if ((info == null) || (info.seg == seg.nseg)) {
                                rseg = chseg(seg.nseg);
                                if (rseg == null) {
                                    System.out.println("rseg is null " + seg.nseg);
                                    break skip;
                                }
                            } else {
                                if (curseg.id != seg.nseg)
                                    throw (new AssertionError());
                                Segment nseg = segments.get(info.seg);
                                Coord noff = seg.offs.get(info.seg);
                                Coord soff = seg.noff.sub(noff);
                                merge(nseg, curseg, soff);
                                seg.nseg = nseg.id;
                                seg.noff = noff;
                                rseg = curseg = nseg;
                            }
                        }
                        Coord nc = grid.sc.add(seg.noff);
                        if (info == null) {
                            rseg.include(rgrid, nc);
                            gridinfo.put(rgrid.id, new GridInfo(rgrid.id, rseg.id, nc));
                        }
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }

        Marker prevmark(Marker mark) {
            for (Marker pm : MapFile.this.markers) {
                if ((pm.getClass() != mark.getClass()) || !pm.nm.equals(mark.nm) || !pm.tc.equals(mark.tc))
                    continue;
                if (pm instanceof SMarker) {
                    if (((SMarker) pm).oid != ((SMarker) mark).oid)
                        continue;
                }
                return (pm);
            }
            return (null);
        }

        void importmark(Message data) {
            Marker mark = loadmarker(data);
            ImportedSegment seg = segs.get(mark.seg);
            if ((seg == null) || (seg.noff == null))
                return;
            Coord soff = seg.offs.get(seg.nseg);
            if (soff == null)
                return;
            mark.tc = mark.tc.add(soff.mul(cmaps));
            mark.seg = seg.nseg;
            if (filter.includemark(mark, prevmark(mark))) {
                add(mark);
            }
        }

        void reimport(boolean errors, Message data) throws InterruptedException {
            if (!Arrays.equals(EXPORT_SIG, data.bytes(EXPORT_SIG.length)))
                throw (new Message.FormatError("Invalid map file format"));
            data = new ZMessage(data);
            try {
                while (!data.eom()) {
                    String type = data.string();
                    int len = data.int32();
                    Message lay = new LimitMessage(data, len);
                    if (type.equals("grid")) {
                        try {
                            importgrid(errors, lay);
                        } catch (RuntimeException exc) {
                            filter.handleerror(exc, "grid");
                        }
                    } else if (type.equals("mark")) {
                        try {
                            importmark(lay);
                        } catch (RuntimeException exc) {
                            filter.handleerror(exc, "mark");
                        }
                    }
                    lay.skip();
                    Utils.checkirq();
                }
            } catch (InterruptedException e) {
                flush();
                throw (e);
            }
            flush();
        }

    }

    public void reimport(boolean errors, Message data, ImportFilter filter) throws InterruptedException {
        new Importer(filter).reimport(errors, data);
    }

    public void reimport(boolean errors, InputStream fp, ImportFilter filter) throws InterruptedException {
        reimport(errors, new StreamMessage(fp, null), filter);
    }

    private static final Coord[] inout = new Coord[]{
            new Coord(0, 0),
            new Coord(0, -1), new Coord(1, 0), new Coord(0, 1), new Coord(-1, 0),
            new Coord(1, -1), new Coord(1, 1), new Coord(-1, 1), new Coord(-1, -1),
    };

    public void update(MCache map, Coord cgc) {
        Collection<MCache.Grid> grids = new ArrayList<>();
        Loading error = null;
        for (Coord off : inout) {
            Coord gc = cgc.add(off);
            try {
                grids.add(map.getgrid(gc));
            } catch (Loading l) {
                error = l;
            }
        }

        if (error != null) {
            map.sendreqs();
            error.waitfor(() -> update(map, cgc), w -> {
            });
        } else {
            if (!grids.isEmpty()) {
                synchronized (procmon) {
                    updqueue.add(new Pair<>(map, grids));
                    process();
                }
            }
        }
    }

    public static Color highlightColor = new Color(Utils.getprefi("highlightTileColor", new Color(255, 0, 255).getRGB()), true);
}
