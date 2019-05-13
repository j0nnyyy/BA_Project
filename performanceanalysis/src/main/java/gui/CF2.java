package gui;

import core.Axis;
import core.DataPlotter;
import core.containers.DataContainer;
import org.knowm.xchart.XChartPanel;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ItemEvent;
import java.util.ArrayList;

public class CF2 extends JFrame {

    private static final String[] aggregations = {"avg", "min", "max"};
    private int width, height;
    private String logPath;
    private DataContainer container;
    private Axis currentAxis = Axis.WORKERS;

    private JPanel pInfo;
    private JPanel pDescription;
    private JPanel pDatasize;
    private JPanel pWorkers;
    private JPanel pCores;
    private JPanel pAggregation;
    private XChartPanel pChart;

    private ButtonGroup bgrWorkers;
    private ButtonGroup bgrCores;
    private ButtonGroup bgrDatasize;
    private ButtonGroup bgrAggregations;

    private ArrayList<String> selectedDescriptions = new ArrayList<>();
    private ArrayList<Long> selectedDataSizes = new ArrayList<>();
    private ArrayList<Integer> selectedWorkers = new ArrayList<>();
    private int selectedCores = -1;
    private String selectedAggregation = "avg";

    public CF2(int width, int height, String logPath) {

        super();
        this.width = width;
        this.height = height;
        this.logPath = logPath;
        init();

    }

    private void init() {

        container = DataContainer.instance();
        container.loadInformation(logPath);

        initMenuBar();
        initInfoPanel();
        initDescriptionPanel();

        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setSize(width, height);
        setLocationRelativeTo(null);
        setVisible(true);

    }

    private void initMenuBar() {

        JMenuBar mb = new JMenuBar();
        JMenu m = new JMenu("X-Axis");
        JMenuItem mI = new JMenuItem("X: Data size");
        mI.addActionListener(e -> {
            currentAxis = Axis.DATASIZE;
            updateDescription(null);
        });
        m.add(mI);
        mI = new JMenuItem("X: Workers");
        mI.addActionListener(e -> {
            currentAxis = Axis.WORKERS;
            updateDescription(null);
        });
        m.add(mI);
        mI = new JMenuItem("X: Cores");
        mI.addActionListener(e -> {
            currentAxis = Axis.CORES;
            updateDescription(null);
        });
        m.add(mI);
        mb.add(m);
        setJMenuBar(mb);

    }

    private void initInfoPanel() {

        pInfo = new JPanel();
        pInfo.setLayout(new BoxLayout(pInfo, BoxLayout.Y_AXIS));
        pInfo.setBorder(BorderFactory.createLineBorder(Color.BLACK));
        add(pInfo, BorderLayout.WEST);

    }

    private void initDescriptionPanel() {

        pDescription = new JPanel();
        pDescription.setLayout(new BoxLayout(pDescription, BoxLayout.Y_AXIS));

        pDescription.add(new JLabel("Description:"));

        for(String description : container.getDescriptions()) {
            JCheckBox cb = new JCheckBox(description);
            cb.addItemListener(e -> updateDescription(e));
            pDescription.add(cb);
        }

        JSeparator separator = new JSeparator();
        Dimension size = new Dimension(
                separator.getMaximumSize().width,
                separator.getPreferredSize().height);
        separator.setMaximumSize(size);
        pDescription.add(separator);

        pInfo.add(pDescription);

    }

    private void initDataSizePanel() {

        if(pDatasize != null) {
            pInfo.remove(pDatasize);
        }

        if(selectedDescriptions.size() == 0) {
            revalidate();
            repaint();
            return;
        }

        selectedDataSizes.clear();

        pDatasize = new JPanel();
        pDatasize.setLayout(new BoxLayout(pDatasize, BoxLayout.Y_AXIS));

        pDatasize.add(new JLabel("Filecount:"));

        if(selectedDescriptions.size() == 1) {
            for(Long l : container.getDataSizes(selectedDescriptions.get(0))) {
                JCheckBox cb = new JCheckBox("" + l);
                cb.addItemListener(e -> updateDataSize(e));
                pDatasize.add(cb);
            }
        } else if(selectedDescriptions.size() > 1) {
            bgrDatasize = new ButtonGroup();

            for(Long l : container.getCommonDataSizes(selectedDescriptions)) {
                JRadioButton rb = new JRadioButton("" + l);
                rb.addItemListener(e -> updateDataSize(e));
                pDatasize.add(rb);
                bgrDatasize.add(rb);
            }
        }

        JSeparator separator = new JSeparator();
        Dimension size = new Dimension(
                separator.getMaximumSize().width,
                separator.getPreferredSize().height);
        separator.setMaximumSize(size);
        pDatasize.add(separator);

        pInfo.add(pDatasize);
        revalidate();
        repaint();

    }

    private void initWorkerPanel() {

        if(pWorkers != null) {
            pInfo.remove(pWorkers);
        }

        if(selectedDescriptions.size() == 0) {
            revalidate();
            repaint();
            return;
        }

        selectedWorkers.clear();

        pWorkers = new JPanel();
        pWorkers.setLayout(new BoxLayout(pWorkers, BoxLayout.Y_AXIS));

        pWorkers.add(new JLabel("Worker count:"));

        if(selectedDescriptions.size() == 1) {
            for(Integer i : container.getAllWorkerCounts(selectedDescriptions.get(0))) {
                JCheckBox cb = new JCheckBox("" + i);
                cb.addItemListener(e -> updateWorkers(e));
                pWorkers.add(cb);
            }
        } else if(selectedDescriptions.size() > 1) {
            bgrWorkers = new ButtonGroup();

            for(Integer i : container.getCommonWorkerCounts(selectedDescriptions)) {
                JRadioButton rb = new JRadioButton("" + i);
                rb.addItemListener(e -> updateWorkers(e));
                pWorkers.add(rb);
                bgrWorkers.add(rb);
            }
        }

        JSeparator separator = new JSeparator();
        Dimension size = new Dimension(
                separator.getMaximumSize().width,
                separator.getPreferredSize().height);
        separator.setMaximumSize(size);
        pWorkers.add(separator);

        pInfo.add(pWorkers);

    }

    private void initCorePanel() {

        if(pCores != null) {
            pInfo.remove(pCores);
        }

        if(selectedDescriptions.size() == 0) {
            revalidate();
            repaint();
            return;
        }

        selectedCores = -1;

        pCores = new JPanel();
        pCores.setLayout(new BoxLayout(pCores, BoxLayout.Y_AXIS));

        pCores.add(new JLabel("Core count:"));

        bgrCores = new ButtonGroup();

        for(Integer i : container.getCommonCoreCounts(selectedDescriptions)) {
            JRadioButton rb = new JRadioButton("" + i);
            rb.addItemListener(e -> updateCores(e));
            pCores.add(rb);
            bgrCores.add(rb);
        }

        JSeparator separator = new JSeparator();
        Dimension size = new Dimension(
                separator.getMaximumSize().width,
                separator.getPreferredSize().height);
        separator.setMaximumSize(size);
        pCores.add(separator);

        pInfo.add(pCores);

        revalidate();
        repaint();

    }

    private void initAggregationPanel() {

        if(pAggregation != null) {
            pInfo.remove(pAggregation);
        }

        if(selectedDescriptions.size() == 0) {
            revalidate();
            repaint();
            return;
        }

        selectedAggregation = "avg";

        pAggregation = new JPanel();
        pAggregation.setLayout(new BoxLayout(pAggregation, BoxLayout.Y_AXIS));

        pAggregation.add(new JLabel("Aggregations:"));

        bgrAggregations = new ButtonGroup();

        for(int i = 0; i < aggregations.length; i++) {
            JRadioButton rb = new JRadioButton("" + aggregations[i]);
            rb.addItemListener(e -> updateAggregation(e));
            pAggregation.add(rb);
            bgrAggregations.add(rb);
        }

        JSeparator separator = new JSeparator();
        Dimension size = new Dimension(
                separator.getMaximumSize().width,
                separator.getPreferredSize().height);
        separator.setMaximumSize(size);
        pAggregation.add(separator);

        pInfo.add(pAggregation);

        revalidate();
        repaint();

    }

    private void updateDescription(ItemEvent e) {

        if(e != null) {
            JCheckBox tmp = (JCheckBox) e.getSource();

            if (tmp.isSelected()) {
                selectedDescriptions.add(tmp.getText());
            } else {
                selectedDescriptions.remove(tmp.getText());
            }
        } else {
            if(pDatasize != null)
                pInfo.remove(pDatasize);
            if(pCores != null)
                pInfo.remove(pCores);
            if(pAggregation != null)
                pInfo.remove(pAggregation);
            if(pWorkers != null)
                pInfo.remove(pWorkers);
        }

        switch (currentAxis) {
            case CORES:
                initDataSizePanel();
                initAggregationPanel();
                break;
            case WORKERS:
                initDataSizePanel();
                initCorePanel();
                initAggregationPanel();
                break;
            case DATASIZE:
                initWorkerPanel();
                initCorePanel();
                initAggregationPanel();
                break;
        }

        updateChart();

    }

    private void updateDataSize(ItemEvent e) {

        if(e.getSource().getClass().equals(JRadioButton.class)) {
            JRadioButton rb = (JRadioButton) e.getSource();
            selectedDataSizes.clear();
            if(rb.isSelected())
                selectedDataSizes.add(Long.parseLong(rb.getText()));
        } else {
            JCheckBox cb = (JCheckBox) e.getSource();
            if(cb.isSelected()) {
                selectedDataSizes.add(new Long(cb.getText()));
            } else {
                selectedDataSizes.remove(new Long(cb.getText()));
            }
        }

        updateChart();

    }

    private void updateWorkers(ItemEvent e) {

        if(e.getSource().getClass().equals(JRadioButton.class)) {
            JRadioButton rb = (JRadioButton) e.getSource();
            selectedWorkers.clear();
            if(rb.isSelected())
                selectedWorkers.add(new Integer(rb.getText()));
        } else {
            JCheckBox cb = (JCheckBox) e.getSource();
            if(cb.isSelected()) {
                selectedWorkers.add(new Integer(cb.getText()));
            } else {
                selectedWorkers.remove(new Integer(cb.getText()));
            }
        }

        updateChart();

    }

    private void updateCores(ItemEvent e) {

        JRadioButton rb = (JRadioButton) e.getSource();

        if(rb.isSelected()) {
            selectedCores = Integer.parseInt(rb.getText());
        }

        updateChart();

    }

    private void updateAggregation(ItemEvent e) {

        JRadioButton rb = (JRadioButton) e.getSource();

        if(rb.isSelected()) {
            selectedAggregation = rb.getText();
        }

        updateChart();

    }

    private void updateChart() {

        if(selectedDescriptions.size() == 0)
            return;

        switch (currentAxis) {
            case DATASIZE:
                if(selectedWorkers.size() == 0 || selectedCores == -1 || selectedAggregation.equals("")) {
                    return;
                }
                break;
            case CORES:
                if(selectedDataSizes.size() == 0 || selectedAggregation.equals("")) {
                    return;
                }
                break;
            case WORKERS:
                if(selectedDataSizes.size() == 0 || selectedCores == -1 || selectedAggregation.equals("")) {
                    return;
                }
                break;
        }

        if(pChart != null)
            remove(pChart);

        pChart = new XChartPanel(DataPlotter.createChart(container, selectedDescriptions, selectedDataSizes, selectedWorkers, selectedCores, currentAxis, selectedAggregation));
        add(pChart, BorderLayout.CENTER);
        revalidate();
        repaint();

    }

    public static void main(String[] args) {

        EventQueue.invokeLater(() -> new CF2(1000, 600, "C:/Users/Jonas Seitz/Desktop/perf_log.txt"));

    }

}
