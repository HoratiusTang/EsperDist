package dist.esper.monitor.ui;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.*;

public class Demo {
	Display display;
	Shell shell;
	Composite composite;
	
	public static void main(String[] args){
		Demo win=new Demo();
	}
	public Demo(){
		init();
	}
	public void init(){
		display=new Display();
		shell=new Shell(display);
		shell.setSize(500, 400);
		shell.setLayout(new FillLayout());
		SashForm form = new SashForm(shell, SWT.HORIZONTAL | SWT.SMOOTH);
	    form.setLayout(new FillLayout());
	    shell.setText("test");

	    Composite child1 = new Composite(form, SWT.NONE);
	    child1.setLayout(new FillLayout());
	    new Label(child1, SWT.NONE).setText("Label in pane 1");

	    Composite child2 = new Composite(form, SWT.NONE);
	    child2.setLayout(new FillLayout());
	    new Button(child2, SWT.PUSH).setText("Button in pane2");

	    Composite child3 = new Composite(form, SWT.NONE);
	    child3.setLayout(new FillLayout());
	    new Label(child3, SWT.PUSH).setText("Label in pane3");

	    form.setWeights(new int[] { 30, 40, 30 });
		
		shell.open();
		while (!shell.isDisposed()) {
			while (!display.readAndDispatch()) {
				display.sleep();
			}
		}
		display.dispose();
	}
}
