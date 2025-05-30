# MySQL Binlog to Avro JSON Comparison Toolset

This toolset helps you compare data consistency between MySQL binary logs and Avro records (converted to JSON) that are expected to correspond to those binlog events, primarily by matching timestamps and other metadata.

It uses a combination of Go programs and shell scripts to:
1.  Convert local MySQL binary log files into a readable JSON format (`binlog_metadata.json`).
2.  Convert Avro files into a combined JSON lines file (`avro_rows.json`).
3.  Compare these two JSON datasets to find matches, discrepancies, and events unique to either source.

It is recommended that you try this in a dev environment to avoid any impact to your production instance. It is also recommended that you keep all scripts within the same folder (e.g., `~/mysql_binlog_comparator`)

## DISCLAIMER

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

## 1. Prerequisites: Installing Go

You need Go installed to build the custom parser and comparison tools. Version 1.22 or newer is recommended.

* **Download Go:** Visit the official Go downloads page: [https://go.dev/dl/](https://go.dev/dl/)
* **Installation Instructions:** Follow the official installation instructions for your operating system: [https://go.dev/doc/install](https://go.dev/doc/install)
* **Verify Installation:** After installation, open a new terminal and type:
    ```bash
    go version
    ```
    You should see the installed Go version. Ensure your `GOPATH` and `GOROOT` environment variables are set up correctly, and that Go's `bin` directory is in your system's `PATH` (the installer usually handles this).

---
## 2. Installing `go-mysql` Toolset (specifically `go-binlogparser`)

The comparison process uses `go-binlogparser` to read local MySQL binary log files. This tool is part of the `go-mysql` project.

1.  **Clone the Repository:**
    Download the repository using Git.
    ```bash
    git clone https://github.com/go-mysql-org/go-mysql.git
    cd go-mysql
    ```

2.  **Build `go-binlogparser`:**
    Build the toolset you just downloaded:
    ```bash
    make build
    ```
    This creates an executable named `go-binlogparser` in a new bin directory (usually `~/mysql_binlog_comparator/go-mysql/bin/go-binlogparser`).
    Make sure to run pwd to determine the absolute path as it will be used later.


---
## 3. Extracting MySQL Binlog Files

You need to obtain the binary log files from your MySQL server and place them into a local directory.

1.  **Locate Binlogs on the Server:**
    Log in to your MySQL server and find the location of your binary logs:
    ```sql
    SHOW VARIABLES LIKE 'log_bin';         -- Shows if binary logging is ON
    SHOW VARIABLES LIKE 'log_bin_basename'; -- Shows the base name and path to binlogs
    SHOW VARIABLES LIKE 'datadir';          -- Often binlogs are in a subdirectory of datadir
    SHOW BINARY LOGS;                     -- Lists the current binlog files
    ```
2.  **Ensure Data is Flushed (Optional but Recommended):**
    To ensure all recent data is written to the current binlog file before copying:
    ```sql
    FLUSH BINARY LOGS;
    ```
    This will close the current binlog file and open a new one.
3.  **Copy Binlog Files:**
    Access your MySQL server's file system (e.g., via SSH). Navigate to the directory containing the binlog files (identified in step 1). Copy all relevant `mysql-bin.XXXXXX` files to a new, dedicated local folder on the machine where you'll run the comparison.
    * **Example Local Folder:** Create a directory, for instance, `~/my_mysql_binlogs`. The `comparator.sh` script will need to know this path.
    * **Copy Command (example from server):**
        ```bash
        # On the MySQL server, after navigating to the binlog directory:
        scp mysql-bin.0000* your_user@your_comparison_machine_ip:~/my_mysql_binlogs/
        ```
        Or use `rsync` or any other preferred method. Ensure the user running the `comparator.sh` script has read permissions for these copied files.

---
## 4. Building Custom Go Tools (`json_parser.go`, `compare_timestamps.go`)

You have two custom Go programs:
* `json_parser.go`: Parses the text output of `go-binlogparser` into JSON lines.
* `compare_timestamps.go`: Compares the JSON from binlogs with the JSON from Avro records.

1.  **Save the Go Files:**
    Place `json_parser.go` and `compare_timestamps.go` in your main toolset directory (e.g., `~/mysql_binlog_comparator`).
2.  **Build the Executables:**
    Open your terminal, navigate to this directory, and run:
    ```bash
    go build -o json_parser json_parser.go
    go build -o compare_timestamps compare_timestamps.go
    ```
    This will create `json_parser` and `compare_timestamps` in the same directory. The `comparator.sh` script will expect them there (or in a `./bin` subdirectory if you prefer to move them).

---
## 5. Preparing Avro Data (Converting Avro to JSON Lines)

The comparison tool expects Avro data to be in a JSON Lines format (one JSON object per line). You'll use the provided `avro_to_json.sh` script for this.

1.  **Download `avro-tools.jar`:**
    * Obtain `avro-tools.jar` (e.g., version 1.11.1 or similar) from the Apache Avro™ website or a trusted source.
    * Download page: [https://avro.apache.org/releases.html](https://avro.apache.org/releases.html) (look under "Avro Tools JAR").
2.  **Place `avro-tools.jar`:**
    * Place the downloaded `avro-tools-X.Y.Z.jar` file in the **same directory** as the `avro_to_json.sh` script. The script is configured to look for it there.
    * **Important:** The script `avro_to_json.sh` uses a wildcard `avro-tools-*.jar` to find the JAR. Ensure your JAR filename matches this pattern or update the `AVRO_TOOLS_JAR` variable in `avro_to_json.sh` if needed. Alternatively, you can use version 1.11.1 as this is the version that is programmed to use.
3.  **Place Your `.avro` Files:**
    * Create a directory named `avro_flat` in your **home directory** (e.g., `~/avro_flat`).
    * Place all your `.avro` files that you want to convert and compare into this `~/avro_flat` directory.
    * If you want to use a different directory, you'll need to modify the `AVRO_FILES_DIRECTORY` variable at the top of `avro_to_json.sh`.
4.  **Make `avro_to_json.sh` Executable:**
    ```bash
    chmod +x avro_to_json.sh
    ```
5.  **Run the Conversion Script:**
    Execute the script from the directory where it's located:
    ```bash
    ./avro_to_json.sh
    ```
    This will:
    * Find all `.avro` files in `AVRO_FILES_DIRECTORY`.
    * Convert each one to JSON Lines format.
    * Concatenate all the JSON output into a single file named `avro_rows.json` in the current directory (where `avro_to_json.sh` was run).

    Review the output of the script for any errors. The `avro_rows.json` file is then ready to be used by `comparator.sh`.

---
## 6. Running the Comparison (`comparator.sh`)

Once Go is installed, `go-binlogparser` is built and placed, your custom Go tools (`json_parser`, `compare_timestamps`) are built, your MySQL binlog files are extracted locally, and your Avro data is converted to `avro_rows.json`, you can run the main comparison.

The `comparator.sh` script (provided in the next section) automates the process.

1.  **Configure `comparator.sh`:**
    Open `comparator.sh` in a text editor. At the top of the script, you'll find a configuration section. **Review and update these variables** to match your environment:
    * `LOCAL_BINLOG_FILES_DIR`: Path to the directory where you saved your extracted MySQL binlog files.
    * `AVRO_JSON_INPUT_FILE`: Path to the `avro_rows.json` file generated by `avro_to_json.sh`.
    * Paths to executables (`GO_BINLOGPARSER_CMD`, `JSON_PARSER_CMD`, `COMPARATOR_CMD`) if you placed them in locations different from the defaults (e.g., defaults assume they are in the same directory as `comparator.sh` or a `./bin` subdir).

2.  **Make `comparator.sh` Executable:**
    ```bash
    chmod +x comparator.sh
    ```

3.  **Run the Script:**
    ```bash
    ./comparator.sh
    ```
    The script will:
    * Iterate through each binlog file in `LOCAL_BINLOG_FILES_DIR`.
    * For each file, run `go-binlogparser`, pipe its output to `json_parser` (passing the current binlog filename as an argument), and append the resulting JSON to `binlog_metadata.json`.
    * After processing all binlog files, it will run `compare_timestamps` using the generated `binlog_metadata.json` and your `avro_rows.json`.
    * Normal progress and the final comparison summary will be printed to the console.
    * Any detailed errors or debug output from the `go-binlogparser | json_parser` pipeline or from `compare_timestamps` will be redirected to `debug_log.txt`.

---
## Understanding the Output

* **Console Output:** Shows progress messages from `comparator.sh` and the final comparison summary from `compare_timestamps`.
* `binlog_metadata.json`: Contains the JSON Lines representation of your MySQL binlog events, processed by `go-binlogparser` and `json_parser`. This is one of the two main inputs for the final comparison.
* `avro_rows.json`: Contains the JSON Lines representation of your Avro data, generated by `avro_to_json.sh`. This is the other main input for the final comparison.
* `debug_log.txt`: Contains any standard error output from the binlog processing pipeline and the comparison tool. Check this file if you encounter issues or unexpected results.
* `pipeline_stderr.log` (from `avro_to_json.sh`): Contains errors specifically from the Avro to JSON conversion pipeline, if any. (Self-correction: `avro_to_json.sh` output errors to console and exits, this file is not generated by it. The `comparator.sh` will generate `debug_log.txt`).