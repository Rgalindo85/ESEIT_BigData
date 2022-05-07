import logging
import pandas as pd

def draw_bar_plot(data, col, ax, color='red', with_annotations=True, ylabel='counts', title=None):
    """Draw a bar plot from given data frame and colunm to plot

    Args:
        data (pandas.dataframe): Dataframe with the data to draw
        col (str): column name
        color (str, optional): color for bars. Defaults to 'red'.
        figsize (tuple, optional): size of the plot, similar format for matplotlib. Defaults to (10, 5).
        with_annotations (bool, optional): To decide to draw annotations over the bars. Defaults to True.
        ylabel (str, optional): label to draw in y-axis. Defaults to 'counts'.
        title (_type_, optional): title of the plot. Defaults to None.

    Returns:
        matplotlib.axis: plot
    """
    
    barplot = pd.DataFrame(data[col].value_counts()).plot(
        kind    = 'bar', 
        color   = color, 
        ax = ax
    )

    # make annotations
    if with_annotations == True:
        for p in barplot.patches:
            b = p.get_bbox()
            y_value = int(b.y1-b.y0)
            barplot.annotate(
                y_value, 
                ( b.x0, b.y1 ),
                color='blue' 
            )
    else:
        pass
    
    barplot.set(title = title, ylabel=ylabel, xlabel=col)
    barplot.grid()

    return barplot


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.captureWarnings(True)
    logging.basicConfig(
        level=logging.INFO,
        format=log_fmt
    )