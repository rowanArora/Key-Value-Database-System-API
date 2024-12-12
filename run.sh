#!/bin/bash

# Check if the build directory exists; create if not
if [ ! -d ./build ]; then
    mkdir ./build
fi

# Navigate to the build directory
cd ./build || exit 1

# Clean the build directory
echo "Removing previous build files..."
rm -rf *

echo "Finished removing previous build files. Running CMake now..."

# Run CMake to configure the project
cmake .. > cmake_log.txt 2>&1
if [ $? -ne 0 ]; then
    echo "CMake configuration failed. Check cmake_log.txt for details."
    exit 1
fi

echo "Finished running CMake. Running Make now..."

# Build the project using make
make > make_log.txt 2>&1
if [ $? -ne 0 ]; then
    echo "Build failed. Check make_log.txt for details."
    exit 1
fi

echo -e "Build succeeded. Running tests...\n"

# Prompt the user to select an executable
echo "Select an executable to run:"
echo "1. Step 2 Experiments"
echo "2. Step 3 Experiments"
echo "3. Tests"
echo "4. Interactive Terminal (Main Project)"
read -p "Enter your choice (1-4): " choice

# Run the selected executable
case $choice in
    1)
        if [ -f ./experiment1 ]; then
            ./experiment1
        else
            echo "Experiment One executable not found!"
            exit 1
        fi
        ;;
    2)
        if [ -f ./experiment2 ]; then
            ./experiment2
        else
            echo "Experiment Two executable not found!"
            exit 1
        fi
        ;;
    3)
        if [ -f ./tests ]; then
            ./tests
        else
            echo "Tests executable not found!"
            exit 1
        fi
        ;;
    4)
        if [ -f ./main_project ]; then
            ./main_project
        else
            echo "Main project executable not found!"
            exit 1
        fi
        ;;
    *)
        echo "Invalid choice. Exiting."
        exit 1
        ;;
esac
