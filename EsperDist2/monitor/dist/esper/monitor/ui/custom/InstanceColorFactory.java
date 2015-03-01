package dist.esper.monitor.ui.custom;

import java.util.*;

import org.eclipse.nebula.widgets.nattable.util.GUIHelper;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.RGB;
import dist.esper.core.cost.InstanceStat;
import dist.esper.core.cost.RawStreamStat;
import dist.esper.core.flow.container.*;
import dist.esper.core.flow.stream.RawStream;

public class InstanceColorFactory {
	static Map<String, ColorCalculator> map;
	static{
		ColorCalculator[] colorCalcs={
			new ColorCalculator(0xFF, 0xCC, 0xCC, 0, -4, -4, 0.0),
			new ColorCalculator(0xCC, 0xFF, 0xCC, -4, 0, -4, 0.0),
			new ColorCalculator(0xCC, 0xFF, 0xFF, -4, 0, 0, 0.0),
			new ColorCalculator(0xFF, 0xFF, 0xCC, 0, 0, -4, 0.0),
			new ColorCalculator(0xCC, 0xCC, 0xCC, 0, 0, -4, 0.0),
			new ColorCalculator(0xCC, 0xCC, 0xFF, -4, -4, 0, 0.0),
		};
		
		map=new TreeMap<String, ColorCalculator>();
		map.put(RawStream.class.getSimpleName(), colorCalcs[0]);
		map.put(FilterStreamContainer.class.getSimpleName(), colorCalcs[1]);
		map.put(FilterDelayedStreamContainer.class.getSimpleName(), colorCalcs[2]);
		map.put(JoinStreamContainer.class.getSimpleName(), colorCalcs[3]);
		map.put(JoinDelayedStreamContainer.class.getSimpleName(), colorCalcs[4]);
		map.put(RootStreamContainer.class.getSimpleName(), colorCalcs[5]);
	}
	
	public static Color getRawStreamColor(RawStreamStat rawStat){
		ColorCalculator colorCalc=map.get(RawStream.class.getSimpleName());
		RGB rgb=colorCalc.getRGB(rawStat.getOutputRateSec());
		return GUIHelper.getColor(rgb);
	}
	
	public static Color getInstanceColor(InstanceStat insStat){
		ColorCalculator colorCalc=map.get(insStat.getType());
		RGB rgb=colorCalc.getRGB(insStat.getOutputRateSec());
		return GUIHelper.getColor(rgb);
	}
}

class ColorCalculator{
	int red0;
	int green0;
	int blue0;
	
	int dRed;
	int dGreen;
	int dBlue;
	
	double base;
	//double step;
	public ColorCalculator(int red0, int green0, int blue0, 
			int dRed, int dGreen, int dBlue, 
			double base) {
		super();
		this.red0 = red0;
		this.green0 = green0;
		this.blue0 = blue0;
		this.dRed = dRed;
		this.dGreen = dGreen;
		this.dBlue = dBlue;
		this.base = base;
		//this.step = step;
	}
	
	public RGB getRGB(double data){
		double diff=data-base;
		int r= red0 + (int)(dRed * diff);
		int g= green0 + (int)(dGreen * diff);
		int b= blue0 + (int)(dBlue * diff);
		
		return new RGB(correct(r), correct(g), correct(b));
	}
	
	private int correct(int c){
		if(c>255)
			return 255;
		if(c<0)
			return 0;
		return c;
	}
}
