package dist.esper.epl.expr;


import java.util.ArrayList;

import com.espertech.esper.epl.spec.FilterStreamSpecCompiled;
import com.espertech.esper.epl.spec.PatternStreamSpecCompiled;
import com.espertech.esper.epl.spec.StatementSpecCompiled;
import com.espertech.esper.epl.spec.StreamSpecCompiled;
import com.espertech.esper.client.soda.*;

import dist.esper.epl.expr.util.IClauseVisitor;
import dist.esper.epl.sementic.StatementSementicWrapper;

public class FromClause extends AbstractClause{
	ArrayList<StreamSpecification> streamSpecList=new ArrayList<StreamSpecification>(4);
	
	public FromClause(){
		type="from";
	}
	
	public void addStreamSpec(StreamSpecification ss){
		streamSpecList.add(ss);
	}
	
	public ArrayList<StreamSpecification> getStreamSpecList() {
		return streamSpecList;
	}

	public void setStreamSpecList(ArrayList<StreamSpecification> streamSpecList) {
		this.streamSpecList = streamSpecList;
	}


	public static class Factory{
		public static FromClause make(StreamSpecCompiled[] sscs){
			FromClause fromClause=new FromClause();
			for(StreamSpecCompiled streamSpec: sscs){
				StreamSpecification streamSpecfication=null;
				if(streamSpec instanceof FilterStreamSpecCompiled){
					FilterStreamSpecCompiled fssc=(FilterStreamSpecCompiled)streamSpec;
					streamSpecfication=FilterStreamSpecification.Factory.make(fssc);
				}
				else if(streamSpec instanceof PatternStreamSpecCompiled){
					PatternStreamSpecCompiled pssc=(PatternStreamSpecCompiled)streamSpec;
					streamSpecfication=PatternStreamSpecification.Factory.make(pssc);
				}
				fromClause.addStreamSpec(streamSpecfication);
			}
			return fromClause;
		}
		
		public static FromClause make(com.espertech.esper.client.soda.FromClause fc0){
			FromClause fc1=new FromClause();
			for(com.espertech.esper.client.soda.Stream s0: fc0.getStreams()){
				StreamSpecification s1=null;
				if(s0 instanceof FilterStream){
					s1=FilterStreamSpecification.Factory.make((FilterStream)s0);
				}
				else{
					throw new RuntimeException("not implemented yet");
					//TODO: s1=PatternStreamSpecification.Factory.make((PatternStream)s0);
				}
				fc1.addStreamSpec(s1);
			}
			return fc1;
		}
	}

	@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append(type);
		String delimiter=" ";
		for(StreamSpecification streamSpec: streamSpecList){
			sw.append(delimiter);
			streamSpec.toStringBuilder(sw);
			delimiter=", ";
		}
	}

	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param)
			throws Exception {
		super.resolve(ssw, param);
		for(StreamSpecification streamSpec: streamSpecList){
			streamSpec.resolve(ssw, param);
		}
		return true;
	}
	
	@Override
	public <T> T accept(IClauseVisitor<T> visitor){
		return visitor.visitFromClause(this);
	}
}
