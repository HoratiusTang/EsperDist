package dist.esper.core.flow.container;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import dist.esper.core.cost.InstanceStat;
import dist.esper.core.flow.stream.Stream;
import dist.esper.epl.expr.util.DeepCloneFactory;
import dist.esper.epl.expr.util.DeepCloneReplaceFactory;
import dist.esper.epl.expr.util.ExpressionComparator;

/**
 * the execution instance which contains multiple @Stream(s), to avoid redundant computing.
 * each of the streams holds exactly the same conditions and view specifications.
 * e.g. (1) select a.id, b.id from A(age>5).win:time(5 second), B(grade>10).win:time(8 second);
 *      (2) select a.name from A(age>5).win:time(5 second);
 *      the execution plan of epl(1) is: RawStream(A) -> FilterStreamA1(A(age>5).win:time(5 second))   \
 *                                       RawStream(B) -> FilterStreamB1(B(grade>10).win:time(8 second))-> RootStream1;
 *      the execution plan of epl(2) is: RawStream(A) -> FilterStreamA2(A(age>5).win:time(5 second))-> RootStream2;
 *      the FilterStreamA1 and FilterStreamA2 holds the same condition(age>5), 
 *      so they could be placed in the same FilterStreamContainer to avoid redundant computing.
 *      however, the outputs of FilterStreamA1 and FilterStreamA2 are different, 
 *      say 'a.id' and 'a.name' respectively. so the output of the StreamContainer is the union of all outputs. 
 *      
 * @see @Stream
 * @author tjy
 *
 */
public abstract class StreamContainer extends Stream {
	private static final long serialVersionUID = -5003957696744609553L;
	public static DeepCloneFactory cloner=new DeepCloneFactory();
	public static ReentrantLock streamContainersLock=new ReentrantLock();
	List<Long> downContainerIdList=new ArrayList<Long>(8);
	
	public void addDownContainerId(long downContainerId){
		if(!this.downContainerIdList.contains(downContainerId)){
			this.downContainerIdList.add(downContainerId);
		}
	}
	
	public List<Long> getDownContainerIdList() {
		return downContainerIdList;
	}

	public void setDownContainerIdList(List<Long> downContainerIdList) {
		this.downContainerIdList = downContainerIdList;
	}
	
	public abstract void dumpAllUpStreamContainers(List<StreamContainer> dscList);
	public List<StreamContainer> dumpAllUpStreamContainers(){
		List<StreamContainer> dscList=new ArrayList<StreamContainer>();
		dumpAllUpStreamContainers(dscList);
		return dscList;
	}
}
