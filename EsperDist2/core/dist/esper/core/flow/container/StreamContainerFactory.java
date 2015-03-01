package dist.esper.core.flow.container;

import dist.esper.core.flow.container.DerivedStreamContainer.StreamAndMapAndBoolComparisonResult;
import dist.esper.core.flow.stream.*;
import dist.esper.util.CollectionUtils;

/** the factory class for @StreamContainer(s)
 * 
 * @author tjy
 */
public class StreamContainerFactory {
	public static StreamContainer copy(StreamContainer sl, int deepth){
		if(deepth<=0){
			return null;
		}
		StreamContainer sl2=null;
		if(sl instanceof RootStreamContainer){
			RootStreamContainer rsl=(RootStreamContainer)sl;
			RootStreamContainer rsl2=new RootStreamContainer();
			StreamContainer child2=copy(rsl.getUpContainer(), deepth-1);
			rsl2.setUpContainer(child2);
			rsl2.setWhereExprList(rsl.getWhereExprList());
			sl2=rsl2;
		}
		else if(sl instanceof JoinDelayedStreamContainer){
			JoinDelayedStreamContainer jcsl=(JoinDelayedStreamContainer)sl;
			JoinDelayedStreamContainer jcsl2=new JoinDelayedStreamContainer();
			jcsl2.setExtraChildCondList(jcsl.getExtraChildCondList());
			jcsl2.setExtraJoinCondList(jcsl.getExtraJoinCondList());
			jcsl2.setAgent((JoinStreamContainer)copy(jcsl.getAgent(), deepth-1));
			sl2=jcsl2;
		}
		else if(sl instanceof JoinStreamContainer){
			JoinStreamContainer jsl=(JoinStreamContainer)sl;
			JoinStreamContainer jsl2=new JoinStreamContainer();
			for(StreamContainer csl: jsl.getUpContainerList()){
				jsl2.addUpContainerIfNotExists(copy(csl, deepth-1));
			}
			jsl2.setJoinExprList(jsl.getJoinExprList());
			sl2=jsl2;
		}
		else if(sl instanceof FilterDelayedStreamContainer){
			FilterDelayedStreamContainer fcsl=(FilterDelayedStreamContainer)sl;
			FilterDelayedStreamContainer fcsl2=new FilterDelayedStreamContainer();
			fcsl2.setExtraFilterCondList(fcsl.getExtraFilterCondList());
			fcsl2.setAgent((FilterStreamContainer)copy(fcsl.getAgent(), deepth-1));
			sl2=fcsl2;
		}
		else if(sl instanceof FilterStreamContainer){
			FilterStreamContainer fsl=(FilterStreamContainer)sl;
			FilterStreamContainer fsl2=new FilterStreamContainer();
			fsl2.setEventSpec(fsl.getEventSpec());
			fsl2.setRawStream(fsl.getRawStream());
			fsl2.setFilterExpr(fsl.getFilterExpr());
			sl2=fsl2;
		}
		else if(sl instanceof PatternStreamContainer){
			PatternStreamContainer psl=(PatternStreamContainer)sl;
			PatternStreamContainer psl2=new PatternStreamContainer();
			psl2.setPatternNode(psl.getPatternNode());
			psl2.setRawStreamList(psl.getRawStreamList());
			sl2=psl2;
		}
		sl2.setInternalCompositeEvent(sl.getInternalCompositeEvent());
		sl2.setResultElementList(sl.getResultElementList());
		sl2.setUniqueName(sl.getUniqueName());
		sl2.setWorkerId(sl.getWorkerId());
		sl2.setEplId(sl.getEplId());
		sl2.setDownContainerIdList(CollectionUtils.shallowClone(sl.getDownContainerIdList()));
		
		DerivedStreamContainer pcsl=(DerivedStreamContainer)sl;
		DerivedStreamContainer pcsl2=(DerivedStreamContainer)sl2;
		
		pcsl2.setWindowTimeUS(pcsl.getWindowTimeUS());

//		for(StreamLocation streamLocation: pcsl.getStreamLocationList()){
//			StreamLocation streamLocation2=shallowCopy(streamLocation);
//			pcsl2.addStreamLocation(streamLocation2);
//		}
		for(StreamAndMapAndBoolComparisonResult lmcr: pcsl.getDirectReuseStreamMapComparisonResultList()){
			StreamAndMapAndBoolComparisonResult lmcr2=new StreamAndMapAndBoolComparisonResult(
					shallowCopy(lmcr.getFirst()), lmcr.getSecond(), lmcr.getThird());
			pcsl2.addDirectReuseStreamMapComparisonResult(lmcr2);
		}
		for(StreamAndMapAndBoolComparisonResult lmcr: pcsl.getIndirectReuseStreamMapComparisonResultList()){
			StreamAndMapAndBoolComparisonResult lmcr2=new StreamAndMapAndBoolComparisonResult(
					shallowCopy(lmcr.getFirst()), lmcr.getSecond(), lmcr.getThird());
			pcsl2.addIndirectReuseStreamMapComparisonResult(lmcr2);
		}

		return sl2;
	}
	
	public static Stream shallowCopy(Stream sl){
		try {
			Stream sl2=sl.getClass().newInstance();
			//sl2.setCompositeEvent(sl.getCompositeEvent());
			sl2.setResultElementList(sl.getResultElementList());
			sl2.setUniqueName(sl.getUniqueName());
			sl2.setWorkerId(sl.getWorkerId());
			sl2.setEplId(sl.getEplId());
			if(sl2 instanceof BaseStream){
				BaseStream ssl=(BaseStream)sl2;
				ssl.setOptionalStreamName(((BaseStream)sl).getOptionalStreamName());
				ssl.setViewSpecs(((BaseStream)sl).getViewSpecs());
			}
			return sl2;
		}
		catch (InstantiationException e) {				
			e.printStackTrace();
		}
		catch (IllegalAccessException e) {				
			e.printStackTrace();
		}
		return null;
	}
	
//	public static StreamLocationContainer shadowCopy(StreamLocationContainer sl){
//		if(sl instanceof RawStreamLocation){
//			//return new RawStreamLocation((RawStreamLocation)sl);
//			return sl;
//		}
//		else{			
//			try {
//				StreamLocation sl2=sl.getClass().newInstance();
//				sl2.setCompositeEvent(sl.getCompositeEvent());
//				sl2.setResultElementList(sl.getResultElementList());
//				sl2.setUniqueName(sl.getUniqueName());
//				sl2.setWorkerMeta(sl.getWorkerMeta());
//				if(sl2 instanceof SingleEventStreamLocation){
//					SingleEventStreamLocation ssl=(SingleEventStreamLocation)sl2;
//					ssl.setOptionalStreamName(((SingleEventStreamLocation)sl).getOptionalStreamName());
//					ssl.setViewSpecs(((SingleEventStreamLocation)sl).getViewSpecs());
//				}
//				return sl2;
//			}
//			catch (InstantiationException e) {				
//				e.printStackTrace();
//			}
//			catch (IllegalAccessException e) {				
//				e.printStackTrace();
//			}
//			return null;
//		}
//	}
}
