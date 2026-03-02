const PlotlyConfig = {
    defaultLayout: {
        font: {
            family: 'Inter, ui-sans-serif, system-ui, -apple-system, sans-serif',
            color: '#9ca3af',
            size: 12
        },
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)',
        margin: { t: 40, r: 20, b: 40, l: 50 },
        xaxis: {
            gridcolor: 'rgba(255,255,255,0.04)',
            linecolor: 'rgba(255,255,255,0.06)',
            tickfont: { size: 11 }
        },
        yaxis: {
            gridcolor: 'rgba(255,255,255,0.04)',
            linecolor: 'rgba(255,255,255,0.06)',
            tickfont: { size: 11 }
        }
    },
    defaultConfig: {
        displayModeBar: false,
        displaylogo: false,
        responsive: true,
        staticPlot: false,
        scrollZoom: false
    },
    chartColors: [
        '#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A',
        '#19D3F3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52'
    ]
};

function renderChart(containerId, chartData, layout) {
    var container = document.getElementById(containerId);
    if (!container) return;

    var mergedLayout = Object.assign({}, PlotlyConfig.defaultLayout, chartData.layout || {}, layout || {});
    // Force our dark theme overrides
    mergedLayout.paper_bgcolor = 'rgba(0,0,0,0)';
    mergedLayout.plot_bgcolor = 'rgba(0,0,0,0)';
    mergedLayout.font = Object.assign({}, PlotlyConfig.defaultLayout.font, mergedLayout.font || {});
    mergedLayout.xaxis = Object.assign({}, PlotlyConfig.defaultLayout.xaxis, mergedLayout.xaxis || {});
    mergedLayout.yaxis = Object.assign({}, PlotlyConfig.defaultLayout.yaxis, mergedLayout.yaxis || {});

    // Apply distinct colors to bar chart traces
    var traces = chartData.data || [];
    traces.forEach(function(trace, i) {
        if (trace.type === 'bar' && !trace.marker) {
            trace.marker = {};
        }
        if (trace.type === 'bar' && trace.marker) {
            // If there are multiple x values, give each bar a different color
            if (trace.x && trace.x.length > 1 && !trace.marker.color) {
                trace.marker.color = trace.x.map(function(_, j) {
                    return PlotlyConfig.chartColors[j % PlotlyConfig.chartColors.length];
                });
            } else if (!trace.marker.color) {
                trace.marker.color = PlotlyConfig.chartColors[i % PlotlyConfig.chartColors.length];
            }
            // Add rounded corners effect and slight opacity
            if (!trace.marker.line) {
                trace.marker.line = { width: 0 };
            }
            trace.marker.opacity = 0.9;
        }
        if (trace.type === 'scatter' && !trace.marker) {
            trace.marker = { color: PlotlyConfig.chartColors[i % PlotlyConfig.chartColors.length], size: 8 };
        }
    });

    Plotly.newPlot(container, traces, mergedLayout, PlotlyConfig.defaultConfig);
}

function initCharts() {
    document.querySelectorAll('.chart-container').forEach(function(container) {
        var chartDataAttr = container.getAttribute('data-chart');
        if (chartDataAttr) {
            try {
                var chartData = JSON.parse(chartDataAttr);
                var id = container.id || 'chart-' + Math.random().toString(36).substr(2, 9);
                container.id = id;
                renderChart(id, chartData);
                container.removeAttribute('data-chart');
            } catch (e) {
                console.error('Error parsing chart data:', e);
            }
        }
    });
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCharts);
} else {
    initCharts();
}

document.addEventListener('htmx:afterSwap', function() {
    setTimeout(initCharts, 100);
});
