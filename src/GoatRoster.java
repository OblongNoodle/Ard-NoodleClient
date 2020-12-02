/* Preprocessed source code */
/* $use: ui/croster */

import haven.Resource;
import haven.UI;
import haven.res.ui.croster.CattleRoster;
import haven.res.ui.croster.Column;
import haven.res.ui.croster.Entry;
import haven.res.ui.croster.TypeButton;

import java.util.Comparator;
import java.util.List;

public class GoatRoster extends CattleRoster<Goat> {
    public static List<Column> cols = initcols(
            new Column<Entry>("Name", Comparator.comparing((Entry e) -> e.name), 200),

            new Column<Goat>(Resource.local().load("gfx/hud/rosters/quality", 2), Comparator.comparing((Goat e) -> e.q).reversed()),

            new Column<Goat>(Resource.local().load("gfx/hud/rosters/meatquantity", 1), Comparator.comparing((Goat e) -> e.meat).reversed()),
            new Column<Goat>(Resource.local().load("gfx/hud/rosters/milkquantity", 1), Comparator.comparing((Goat e) -> e.milk).reversed()),
            new Column<Goat>(Resource.local().load("gfx/hud/rosters/woolquantity", 1), Comparator.comparing((Goat e) -> e.milk).reversed()),

            new Column<Goat>(Resource.local().load("gfx/hud/rosters/meatquality", 1), Comparator.comparing((Goat e) -> e.meatq).reversed()),
            new Column<Goat>(Resource.local().load("gfx/hud/rosters/milkquality", 1), Comparator.comparing((Goat e) -> e.milkq).reversed()),
            new Column<Goat>(Resource.local().load("gfx/hud/rosters/woolquality", 1), Comparator.comparing((Goat e) -> e.milkq).reversed()),
            new Column<Goat>(Resource.local().load("gfx/hud/rosters/hidequality", 1), Comparator.comparing((Goat e) -> e.hideq).reversed()),

            new Column<Goat>(Resource.local().load("gfx/hud/rosters/breedingquality", 1), Comparator.comparing((Goat e) -> e.seedq).reversed())
    );

    protected List<Column> cols() {
        return (cols);
    }

    public static CattleRoster mkwidget(UI ui, Object... args) {
        return (new GoatRoster());
    }

    public Goat parse(Object... args) {
        int n = 0;
        long id = (Long) args[n++];
        String name = (String) args[n++];
        Goat ret = new Goat(id, name);
        ret.q = ((Number) args[n++]).doubleValue();
        ret.meat = (Integer) args[n++];
        ret.milk = (Integer) args[n++];
        ret.wool = (Integer) args[n++];
        ret.meatq = (Integer) args[n++];
        ret.milkq = (Integer) args[n++];
        ret.woolq = (Integer) args[n++];
        ret.hideq = (Integer) args[n++];
        ret.seedq = (Integer) args[n++];
        return (ret);
    }

    public TypeButton button() {
        return (typebtn(Resource.local().load("gfx/hud/rosters/btn-goat", 3),
                Resource.local().load("gfx/hud/rosters/btn-goat-d", 3)));
    }
}