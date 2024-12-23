
# Function to read the configuration from a file
def read_config(config_file):
    config = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and not line.startswith("#"):
                parameter, value = line.split("=", 1)
                config[parameter.strip()] = value.strip()
    return config
