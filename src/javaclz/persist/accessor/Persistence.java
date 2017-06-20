package javaclz.persist.accessor;

import javaclz.persist.opt.PersistenceOpt;
import javaclz.persist.data.PersistenceData;

import java.util.Collection;

public interface Persistence {
	
	// for once persistence
	void persistenceOne(PersistenceOpt opt, PersistenceData data) throws Exception;
	
	// for batch persistence
	void persistenceBatch(PersistenceOpt opt, Collection<PersistenceData> data) throws Exception;
	
	void close() throws Exception;
}
