package dist.esper.core.flow.centralized;

import java.util.*;

import dist.esper.epl.expr.*;
import dist.esper.epl.expr.util.*;
import dist.esper.epl.expr.util.EventOrPropertySpecComparator.EPSRelation;

import dist.esper.util.CarryIncrementPermutation;
import dist.esper.util.CollectionUtils;

/**
 * the class to build @Tree(s). 
 * the trees are balanced ones, and so it is with any sub-tree inside.
 * 
 * @author tjy
 *
 */
public class TreeBuilder {
	long eplId;
	String epl;
	StatementSpecification statSpec=null;
	List<BaseNode> baseNodeList=null;	
	List<Tree> treeList=new ArrayList<Tree>();;
	List<AbstractBooleanExpression> whereConjunctList=null;
	TreeCostEvaluator costEval=new TreeCostEvaluator();
	static EventOrPropertySpecComparator epsComparator=new EventOrPropertySpecComparator();
	static NodeComparator nodeComp=new NodeComparator();
	static TreeComparator treeComp=new TreeComparator();
	Map<Long,Node[]> map=new HashMap<Long,Node[]>();
	
	/** the final outputs. the '*' are converted to detailed lists. */
	public List<SelectClauseExpressionElement> resultElementList=null;

	public TreeBuilder(long eplId, String epl, StatementSpecification statSpec) {
		super();
		this.eplId = eplId;
		this.epl = epl;
		this.statSpec = statSpec;
	}
	
	public long getEplId() {
		return eplId;
	}

	public void setEplId(long eplId) {
		this.eplId = eplId;
	}
	
	public List<BaseNode> getStreamNodeList() {
		return baseNodeList;
	}

	public List<Tree> getTreeList() {
		return treeList;
	}

	private BaseNode cloneSingleEventNode(BaseNode sn){
		if(sn instanceof FilterNode){
			return new FilterNode((FilterNode)sn);
		}
		else{
			return new PatternNode((PatternNode)sn);
		}
	}
	
	public List<Tree> buildTreeList(){
		whereConjunctList=Collections.emptyList();
		if(statSpec.getWhereClause()!=null){
			whereConjunctList=statSpec.getWhereClause().getConjunctionList();
		}
		buildStreamNodeList();
		buildResultElementList();
		
		for(Node node: baseNodeList){
			this.attachResultElementsToNode(node, whereConjunctList);
		}
		
		Node[] sns=new BaseNode[baseNodeList.size()];
		for(int i=0;i<sns.length;i++){
			sns[i]=cloneSingleEventNode(baseNodeList.get(i));
		}
		buildBottomUp(sns);
		evalAndsortTreeList();
		return treeList;
	}
	
	private void evalAndsortTreeList(){
		for(Tree tree: treeList){
			costEval.evalTree(tree);
		}
		Collections.sort(treeList, treeComp);
	}
	
	private void buildBottomUp(Node[] c){
		if(c.length==1){
			RootNode root=new RootNode(c[0]);
			addNewTree(root);
			return;
		}
		for(int i=0;i<c.length;i++){
			for(int j=i+1;j<c.length;j++){
				if(Math.abs(c[i].getLevel()-c[j].getLevel())<=1){					
					Node[] c2=buildNewNodeArray(c, i, j);
					Arrays.sort(c2, nodeComp);
					int hashCode=hashCode(c2);
					if(!map.containsKey(Long.valueOf(hashCode))){
						map.put((long)hashCode, c2);
						buildBottomUp(c2);
					}
				}
			}
		}
	}
	
	private void addNewTree(RootNode rootNode){
		List<AbstractBooleanExpression> joinExprList=CollectionUtils.shallowClone(this.whereConjunctList);
		attachExpressionsAndResultElementsToNodeBottomUp(rootNode, joinExprList);
		Tree tree=new Tree(eplId, epl, rootNode);
		treeList.add(tree);
	}
	
	/**
	 * use two nodes (indexed by childIndex1 and childIndex2) to form a join node, 
	 * and put the join node at index 0. other nodes stay unchanged.
	 * @param nodes
	 * @param childIndex1
	 * @param childIndex2
	 * @return
	 */
	private Node[] buildNewNodeArray(Node[] nodes, int childIndex1, int childIndex2){
		JoinNode joinNode=newJoinNodeWithoutJoinExpressions(nodes[childIndex1], nodes[childIndex2]);
		Node[] c2=new Node[nodes.length-1];
		c2[0]=joinNode;
		int t=1;
		for(int k=0;k<nodes.length;k++){
			if(k!=childIndex1 && k!=childIndex2){
				c2[t]=nodes[k];
				t++;
			}
		}
		assert(t==c2.length);
		return c2;
	}
	
	private int hashCode(Node[] nodes){
		int h=0;
		for(Node n: nodes){
			h=hashCode(n, h);
		}
		return h;
	}
	
	private int hashCode(Node n, int h){
		if(n instanceof BaseNode){
			return h*31 + (int)n.getId();
		}
		else if(n instanceof JoinNode){
			JoinNode join=(JoinNode)n;
			h = h*31 + 501;
			for(Node child: join.getChildList()){
				h = hashCode(child, h);
			}
			h = h*31 + 997;
			return h;
		}
		return h;
	}
	
	private JoinNode newJoinNodeWithoutJoinExpressions(Node left, Node right){
		JoinNode joinNode=new JoinNode(left, right);
		return joinNode;
	}
	
	private void attachExpressionsAndResultElementsToNodeBottomUp(Node node, List<AbstractBooleanExpression> joinExprList){
		if(node instanceof RootNode){
			RootNode rootNode=(RootNode)node;
			attachExpressionsAndResultElementsToNodeBottomUp(rootNode.getChild(), joinExprList);
			rootNode.setResultElementList(this.resultElementList);
		}
		else if(node instanceof JoinNode){
			JoinNode joinNode=(JoinNode)node;
			for(Node child: joinNode.getChildList()){
				if(child instanceof JoinNode){
					attachExpressionsAndResultElementsToNodeBottomUp((JoinNode)child, joinExprList);
				}
			}
			attachJoinExpressionsToJoinNode(joinNode, joinExprList);
			attachResultElementsToNode(joinNode, joinExprList);
		}
		else{ //node instanceof BaseNode
			/** do nothing, already attached before building tree **/
		}
	}
	
	private void attachJoinExpressionsToJoinNode(JoinNode joinNode, List<AbstractBooleanExpression> joinExprList){
		Set<EventAlias> nodeEaSet=joinNode.dumpSelectedEventAliases();
		List<AbstractBooleanExpression> toRemoveList=new ArrayList<AbstractBooleanExpression>(joinExprList.size());
		
		Set<EventAlias> exprEaSet=new HashSet<EventAlias>();
		for(AbstractBooleanExpression joinExpr: joinExprList){
			exprEaSet.clear();
			/*joinExpr.dumpAllEventAliases(exprEaSet);*/
			EventAliasDumper.dump(joinExpr, exprEaSet);
			if(nodeEaSet.containsAll(exprEaSet)){
				joinNode.addFilterExprssion(joinExpr);
				toRemoveList.add(joinExpr);
			}
		}
		joinExprList.removeAll(toRemoveList);
	}
	
	private void attachResultElementsToNode(Node node, List<AbstractBooleanExpression> joinExprList){		
		Set<EventAlias> nodeEaSet=node.dumpSelectedEventAliases();
		List<EventOrPropertySpecification> nodeResultEpsList=new ArrayList<EventOrPropertySpecification>(4);
		
		Set<EventOrPropertySpecification> tempEpsSet=new HashSet<EventOrPropertySpecification>();//resue the set
		for(SelectClauseExpressionElement se: this.resultElementList){
			tempEpsSet.clear();
			se.dumpAllEventOrPropertySpecReferences(tempEpsSet);
			for(EventOrPropertySpecification eps: tempEpsSet){
				if(nodeEaSet.contains(eps.getEventAlias())){
					nodeResultEpsList.add(eps);
				}
			}
		}
		for(AbstractBooleanExpression bExpr: joinExprList){
			tempEpsSet.clear();
			/*bExpr.dumpAllEventOrPropertySpecReferences(tempEpsSet);*/
			EventOrPropertySpecReferenceDumper.dump(bExpr, tempEpsSet);
			for(EventOrPropertySpecification eps: tempEpsSet){
				if(nodeEaSet.contains(eps.getEventAlias())){
					nodeResultEpsList.add(eps);
				}
			}
		}
		
		nodeResultEpsList=this.removeImplicated(nodeResultEpsList);
		for(EventOrPropertySpecification nodeResultEps: nodeResultEpsList){
			node.addResultElement(new SelectClauseExpressionElement(nodeResultEps));
		}
	}
	
	private void buildResultElementList(){
		boolean isWildcard=false;
		for(SelectClauseElement sce: statSpec.getSelectClause().getElementList()){
			if(sce instanceof SelectClauseWildcardElement){
				isWildcard=true;
				break;
			}
		}
		if(isWildcard){
			resultElementList=new ArrayList<SelectClauseExpressionElement>(baseNodeList.size());
			for(BaseNode sn: baseNodeList){
				if(sn instanceof FilterNode){
					FilterNode fsn=(FilterNode)sn;
					resultElementList.add(new SelectClauseExpressionElement(new EventSpecification(fsn.getEventSpec())));
				}
				else{
					PatternNode psn=(PatternNode)sn;
					/*Set<EventOrPropertySpecification> epsSet=psn.getPatternNode().dumpOwnEventOrPropertySpecReferences();*/
					Set<EventOrPropertySpecification> epsSet=EventOrPropertySpecReferenceDumper.dump(psn.getPatternNode());
					for(EventOrPropertySpecification eps: epsSet){
						resultElementList.add(new SelectClauseExpressionElement(new EventSpecification((EventSpecification)eps)));
					}
				}
			}
		}
		else{
			resultElementList=new ArrayList<SelectClauseExpressionElement>(statSpec.getFromClause().getStreamSpecList().size());
			for(SelectClauseElement sce: statSpec.getSelectClause().getElementList()){
				resultElementList.add((SelectClauseExpressionElement)sce);
			}
		}
	}
	
	private List<EventOrPropertySpecification> removeImplicated(List<EventOrPropertySpecification> epsList){
		boolean[] removeFlags=new boolean[epsList.size()];
		for(int i=0;i<epsList.size();i++){
			removeFlags[i]=false;
			for(int j=0; j<i; j++){
				EPSRelation r=epsComparator.compare(epsList.get(i), epsList.get(j));
				if(r==EPSRelation.EQUAL){
					if(removeFlags[j]){
						removeFlags[i]=true;
						break;
					}
					else{
						removeFlags[j]=true;
					}
				}
				if(r==EPSRelation.CONTAINS){
					removeFlags[j]=true;
				}
				else if(r==EPSRelation.IS_CONTAINED){
					removeFlags[i]=true;
					break;
				}
			}
		}
		List<EventOrPropertySpecification> copyList=new ArrayList<EventOrPropertySpecification>(epsList.size());
		copyList.addAll(epsList);
		epsList.clear();
		for(int i=0;i<copyList.size();i++){
			if(!removeFlags[i]){
				epsList.add(copyList.get(i));
			}
		}
		return epsList;
	}
	
	public void buildStreamNodeList(){
		List<StreamSpecification> ssList=statSpec.getFromClause().getStreamSpecList();
		baseNodeList=new ArrayList<BaseNode>(ssList.size());
		
		for(StreamSpecification ss: ssList){
			if(ss instanceof FilterStreamSpecification){
				FilterStreamSpecification fss=(FilterStreamSpecification)ss;
				FilterNode fsn=new FilterNode(fss.getEventAlias());
				fsn.setViewSpecs(fss.getViewSpecs());
				if(fss.getFilterExpr()!=null){
					fsn.setFilterExprssion(fss.getFilterExpr());
				}
				List<AbstractBooleanExpression> toRemoveList=new ArrayList<AbstractBooleanExpression>(ssList.size()); 
				/**
				 for(AbstractBooleanExpression filterExpr: whereConjunctList){				 
					Set<EventAlias> eaSet=filterExpr.dumpAllEventAliases();
					eaSet.remove(fss.getEventAlias());
					if(eaSet.isEmpty()){
						fsn.addFilterExprssion(filterExpr);
						toRemoveList.add(filterExpr);
					}
				}
				**/
				whereConjunctList.removeAll(toRemoveList);
				baseNodeList.add(fsn);
			}
			else{
				PatternStreamSpecification pss=(PatternStreamSpecification)ss;
				PatternNode psn=new PatternNode(pss.getPatternNode());
				psn.setOptionalStreamName(pss.getOptionalStreamName());
				psn.setViewSpecs(pss.getViewSpecs());
				baseNodeList.add(psn);
			}
		}
	}
}

class NodeComparator implements Comparator<Node>{
	@Override
	public int compare(Node n1, Node n2) {
		int l1=n1.getLevel();
		int l2=n2.getLevel();
		if(l1!=l2){
			return l1-l2;
		}
		else{
			return getMinId(n1)-getMinId(n2);
		}
	}
	
	private int getMinId(Node n){
		if(n instanceof BaseNode){
			return (int)n.getId();
		}
		else{//n instanceof JoinNode
			JoinNode joinNode=(JoinNode)n;
			int minId=Integer.MAX_VALUE;
			for(Node child: joinNode.getChildList()){
				int id=getMinId(child);
				minId=(id<minId)?id:minId;
			}
			return minId;
		}
	}	
}

class TreeComparator implements Comparator<Tree>{
	@Override
	public int compare(Tree t1, Tree t2) {
		double d=t1.getRoughCost() - t2.getRoughCost();
		if(d<0)
			return -1;
		else if(d>0)
			return 1;
		else
			return 0;
	}
}