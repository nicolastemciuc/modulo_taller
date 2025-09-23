require 'csv'

# Regex to parse tcpdump line
LINE_RE = /
  (?<timestamp>\d+:\d+:\d+\.\d+)\s+IP\s+
  (?<src_ip>\d+\.\d+\.\d+\.\d+)\.(?<src_port>\d+)\s+>\s+
  (?<dst_ip>\d+\.\d+\.\d+\.\d+)\.(?<dst_port>\d+):.*length\s+(?<length>\d+)
/x

def parse_tcpdump(input_file, output_file)
  CSV.open(output_file, "w") do |csv|
    # Write CSV header
    csv << ["timestamp", "source_ip", "source_port", "destination_ip", "destination_port", "packet_size"]

    File.foreach(input_file) do |line|
      if (match = LINE_RE.match(line))
        csv << [
          match[:timestamp],
          match[:src_ip],
          match[:src_port],
          match[:dst_ip],
          match[:dst_port],
          match[:length]
        ]
      end
    end
  end
end

if __FILE__ == $0
  parse_tcpdump("packets.log", "output.csv")
end
