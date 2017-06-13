package javaclz.persist;

/**
 * Created by xiaoke on 17-6-9.
 */
public enum PersistenceLevel {

    NONE(false, false), MAIN_ONLY(true, false), BACK_ONLY(false, true), BOTH(true, true), BOTH_FORCE(true, true, true);

    private final boolean main;

    private final boolean backup;

    private final boolean force;

    PersistenceLevel(boolean main, boolean backup, boolean force) {
        this.main = main;
        this.backup = backup;
        this.force = force;
    }

    PersistenceLevel(boolean main, boolean backup) {
        this(main, backup, false);
    }

    public boolean isMain() {
        return main;
    }

    public boolean isBackup() {
        return backup;
    }

    public boolean isForce() {
        return force;
    }
}
