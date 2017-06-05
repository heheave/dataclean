package javaclz.persist;

import javaclz.persist.data.PersistenceData;
import javaclz.persist.opt.PersistenceOpt;

import java.util.Collection;

public interface Persistence {
	
	// persist packets to persistence device
	public void persistence(PersistenceData pds, final PersistenceOpt popt) throws Exception;

	public void persistence(final Collection<PersistenceData> pds, final PersistenceOpt popt) throws Exception;
}
