package gui;

import core.DataPlotter;
import core.InformationContainer;
import org.knowm.xchart.XChartPanel;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ItemEvent;
import java.util.ArrayList;

public class ChartFrame extends JFrame {

    private int width, height;
    private String logPath;

    private InformationContainer container;
    private JPanel pInfo;
    private JPanel pFileCount;
    private JPanel pAggregation;
    private XChartPanel chart;

    private ButtonGroup gFCButtons;
    private ButtonGroup gAggButtons;

    private ArrayList<String> selectedSeries = new ArrayList<>();
    private long selectedFileCount = 0;
    private String selectedAggregation = "avg";

    public ChartFrame(int width, int height, String logPath) {

        super("Dataplotter");
        this.width = width;
        this.height = height;
        this.logPath = logPath;
        init();

    }

    private void init() {

        container = new InformationContainer(logPath);

        initInfoPanel();

        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setSize(width, height);
        setLocationRelativeTo(null);
        setVisible(true);

    }

    private void initInfoPanel() {

        pInfo = new JPanel();
        pInfo.setLayout(new BoxLayout(pInfo, BoxLayout.Y_AXIS));

        pInfo.add(initSeriesPanel());

        pInfo.setBorder(BorderFactory.createLineBorder(Color.BLACK));

        add(pInfo, BorderLayout.WEST);

    }

    private JPanel initSeriesPanel() {

        JPanel tmp = new JPanel();
        tmp.setLayout(new BoxLayout(tmp, BoxLayout.Y_AXIS));

        tmp.add(new JLabel("Series:"));

        for(String sName : container.getSeriesNames()) {
            JCheckBox cb = new JCheckBox(sName);

            cb.addItemListener(e -> updateSeries(e));

            tmp.add(cb);
        }

        JSeparator separator = new JSeparator();
        Dimension size = new Dimension(
                separator.getMaximumSize().width,
                separator.getPreferredSize().height);
        separator.setMaximumSize(size);

        tmp.add(separator);

        return tmp;

    }

    private JPanel initOrUpdateFileCountPanel() {

        JPanel tmp = new JPanel();
        tmp.setLayout(new BoxLayout(tmp, BoxLayout.Y_AXIS));
        gFCButtons = new ButtonGroup();

        tmp.add(new JLabel("File counts:"));

        boolean first = true;

        for(Long fileCount : container.getCommonFileCounts(selectedSeries)) {
            JRadioButton rb = new JRadioButton(fileCount + "");

            if(first) {
                first = false;
                rb.setSelected(true);

                selectedFileCount = fileCount;
            }

            rb.addItemListener(e -> updateFileCount(e));

            gFCButtons.add(rb);
            tmp.add(rb);
        }

        JSeparator separator = new JSeparator();
        Dimension size = new Dimension(
                separator.getMaximumSize().width,
                separator.getPreferredSize().height);
        separator.setMaximumSize(size);

        tmp.add(separator);

        return tmp;

    }

    private JPanel initAggregationPanel() {

        String[] aggregations = {"avg", "min", "max"};
        JPanel tmp = new JPanel();
        tmp.setLayout(new BoxLayout(tmp, BoxLayout.Y_AXIS));
        gAggButtons = new ButtonGroup();

        tmp.add(new JLabel("Aggregations:"));

        for(int i = 0; i < aggregations.length; i++) {
            JRadioButton rb = new JRadioButton(aggregations[i]);

            if(selectedAggregation.equals("") && aggregations[i].equals("avg")) {
                rb.setSelected(true);
            } else if(selectedAggregation.equals(aggregations[i])) {
                rb.setSelected(true);
            }

            rb.addItemListener(e -> updateAggregations(e));

            gAggButtons.add(rb);
            tmp.add(rb);
        }

        JSeparator separator = new JSeparator();
        Dimension size = new Dimension(
                separator.getMaximumSize().width,
                separator.getPreferredSize().height);
        separator.setMaximumSize(size);

        tmp.add(separator);

        return tmp;

    }

    private void updateSeries(ItemEvent e) {

        JCheckBox cb = (JCheckBox) e.getSource();

        if(cb.isSelected()) {
            selectedSeries.add(cb.getText());
        } else {
            selectedSeries.remove(cb.getText());
        }

        if(pFileCount != null) {
            pInfo.remove(pFileCount);
            pFileCount = null;
        }

        if(pAggregation != null) {
            pInfo.remove(pAggregation);
            pAggregation = null;
        }

        if(selectedSeries.size() > 1) {
            pFileCount = initOrUpdateFileCountPanel();
            pInfo.add(pFileCount);

            pAggregation = initAggregationPanel();
            pInfo.add(pAggregation);

            updateChart();
        } else if(selectedSeries.size() == 1) {
            pAggregation = initAggregationPanel();
            pInfo.add(pAggregation);

            updateChart();
        }

        revalidate();
        repaint();

    }

    private void updateFileCount(ItemEvent e) {

        JRadioButton rb = (JRadioButton) e.getSource();

        selectedFileCount = Integer.parseInt(rb.getText());

        updateChart();

    }

    private void updateAggregations(ItemEvent e) {

        JRadioButton rb = (JRadioButton) e.getSource();

        selectedAggregation = rb.getText();

        updateChart();

    }

    private void updateChart() {

        if(chart != null)
            remove(chart);

        if(selectedSeries.size() == 1) {
            chart = new XChartPanel(DataPlotter.createChart(container, selectedSeries.get(0), selectedAggregation));
        } else if(selectedSeries.size() > 1) {
            chart = new XChartPanel(DataPlotter.createChart(container, selectedSeries, selectedAggregation, selectedFileCount));
        } else {

        }

        add(chart);

        revalidate();
        repaint();

    }

    public static void main(String[] args) {
        EventQueue.invokeLater(() -> new ChartFrame(1000, 800, "C:/Users/Jonas/Desktop/BA_Project/log.txt"));
    }

}
