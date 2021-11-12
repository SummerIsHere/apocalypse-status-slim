# Apocalypse Status Dashboard

## Purpose
Climate change and resource depletion are grave long-term dangers.
The code in this repository will pull data from various public sources and display metrics showing how far those dangers are progressing. It also serves as an archive of minimally processed versions of that data. The metrics are displayed in the Power BI report in the report subfolder.

## Documentation
See the [wiki](https://github.com/SummerIsHere/apocalypse-status-slim/wiki) for more detailed documentation.

## Installation Instructions

1. Download and install the [Firefox web browser](https://www.mozilla.org/firefox/). Go to about About Firefox to check whether it is 32-bit or 64-bit. 64-bit recommended throughout this guide.
2. Install the [Anaconda distribution](https://www.anaconda.com/download/) of Python3. When you install, be sure to include the installation of Anaconda Navigator.
3. Open the Anaconda Navigator and install the pandas-datareader package.
3. Install [PowerBI](https://www.powerbi.com), a program to create reports and data visualizations.  It is required to open "Apocalypse Status Board.pbix". This program is Windows only, so if you are on another system, use VirtualBox, Parallels, etc to launch a Windows environment)

## How to run
1. Check that your default Python is Python 3 by typing "python --version" in the terminal
1. Open main.py in your favorite text editor. If you kept things in their default state, you won't need to modify anything but check it over to see if you need to change any variables
2. Run main.py script (and any other scripts) from the base directory of the repository. Do this by opening a terminal console, navigating to the top of the repository, and type "python main.py"
3. Errors will show up in the terminal, logging will be outputted to main_logging.txt
3. Other steps
4. Open up the dashboard in PowerBI

## Notes
