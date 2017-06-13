package javaclz.persist;

import javaclz.persist.data.PersistenceData;
import javaclz.persist.opt.PersistenceOpt;

import java.util.Collection;

public interface Persistence {
	
	// persist packets to persistence device
	void persistence(PersistenceData pds, final PersistenceOpt popt, PersistenceLevel plevel) throws Exception;

	void persistence(final Collection<PersistenceData> pds, final PersistenceOpt popt, PersistenceLevel plevel) throws Exception;
}
