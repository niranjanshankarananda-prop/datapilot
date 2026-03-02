const PlotlyConfig = {
    defaultLayout: {
        font: {
            family: 'ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif'
        },
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)',
        margin: { t: 40, r: 40, b: 40, l: 60 }
    },
    defaultConfig: {
        displayModeBar: true,
        displaylogo: false,
        modeBarButtonsToRemove: ['sendDataToCloud'],
        responsive: true
    },
    chartColors: [
        '#4F46E5',
        '#10B981',
        '#F59E0B',
        '#EF4444',
        '#8B5CF6',
        '#EC4899',
        '#06B6D4',
        '#84CC16'
    ]
};

function renderChart(containerId, chartData, layout = {}) {
    const container = document.getElementById(containerId);
    if (!container) {
        console.error('Chart container not found:', containerId);
        return;
    }

    const mergedLayout = {
        ...PlotlyConfig.defaultLayout,
        ...layout
    };

    Plotly.newPlot(container, chartData.data, mergedLayout, PlotlyConfig.defaultConfig);
}

function exportChartAsPNG(containerId, filename = 'chart') {
    const container = document.getElementById(containerId);
    if (!container) {
        console.error('Chart container not found:', containerId);
        return;
    }

    Plotly.downloadImage(container, {
        format: 'png',
        width: 800,
        height: 600,
        filename: filename
    });
}

function initCharts() {
    document.querySelectorAll('.chart-container').forEach(function(container) {
        const chartDataAttr = container.getAttribute('data-chart');
        if (chartDataAttr) {
            try {
                const chartData = JSON.parse(chartDataAttr);
                const id = container.id || 'chart-' + Math.random().toString(36).substr(2, 9);
                container.id = id;
                renderChart(id, chartData);
                container.removeAttribute('data-chart');
            } catch (e) {
                console.error('Error parsing chart data:', e);
            }
        }
    });

    document.querySelectorAll('.export-chart-btn').forEach(function(btn) {
        btn.addEventListener('click', function() {
            const chartId = btn.getAttribute('data-chart-id');
            const chartContainer = document.getElementById('chart-' + chartId);
            if (chartContainer) {
                exportChartAsPNG('chart-' + chartId, 'chart-' + chartId);
            }
        });
    });
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCharts);
} else {
    initCharts();
}

document.addEventListener('htmx:afterSwap', function(event) {
    setTimeout(initCharts, 100);
});
