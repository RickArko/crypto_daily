import matplotlib.pyplot as plt
from itertools import cycle
from matplotlib.ticker import FuncFormatter

# Set Plot Parameters
plt.style.use('ggplot')
plt.rcParams["figure.figsize"] = (24, 5)
plt.rcParams.update({'font.size': 16})
plt.rcParams.update({'figure.titlesize': 18})
plt.rcParams['font.family'] = "DeJavu Serif"
plt.rcParams['font.serif'] = "Cambria Math"

color_pal = plt.rcParams['axes.prop_cycle'].by_key()['color']
color_cycle = cycle(plt.rcParams['axes.prop_cycle'].by_key()['color'])

def thousands_formatter(x, pos):
    return "{:,.0f}".format(x)


def format_yaxis_thousands(ax):
    ax.yaxis.set_major_formatter(FuncFormatter(thousands_formatter))


def percentage_formatter(x, pos):
    return "{:.0f}%".format(x * 100)  # Assuming x is in decimal form (e.g., 0.25 for 25%)


def format_yaxis_percentage(ax):
    ax.yaxis.set_major_formatter(FuncFormatter(percentage_formatter))
