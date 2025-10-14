require 'csv'

# Flow gaps in microseconds
FLOW_GAPS = [500, 5000, 10000]

# Parse "HH:MM:SS.ssssss" -> float seconds
def parse_time(ts)
  h, m, s = ts.split(":")
  h.to_i * 3600 + m.to_i * 60 + s.to_f
end

def extract_flows(input_file, output_file, flow_gap_us)
  flow_gap_s = flow_gap_us.to_f / 1_000_000.0
  global_flow_id = 0

  # Group packets by (src_ip,src_port,dst_ip,dst_port)
  groups = Hash.new { |h, k| h[k] = [] }

  CSV.foreach(input_file, headers: true) do |row|
    key = [row["source_ip"], row["source_port"], row["destination_ip"], row["destination_port"]]
    groups[key] << {
      time: parse_time(row["timestamp"]),
      size: row["packet_size"].to_i,
      ts_raw: row["timestamp"] # keep original string for output
    }
  end

  CSV.open(output_file, "w") do |csv|
    csv << ["flow_id", "src_ip", "src_port", "dst_ip", "dst_port",
            "start_time", "end_time", "packet_count", "total_size"]

    groups.each do |(src_ip, src_port, dst_ip, dst_port), packets|
      packets.sort_by! { |p| p[:time] }

      current_flow = []
      last_time = nil

      packets.each do |pkt|
        if last_time && (pkt[:time] - last_time > flow_gap_s)
          # close old flow
          csv << [
            global_flow_id, src_ip, src_port, dst_ip, dst_port,
            current_flow.first[:ts_raw], current_flow.last[:ts_raw],
            current_flow.size, current_flow.sum { |x| x[:size] }
          ]
          global_flow_id += 1
          current_flow = []
        end
        current_flow << pkt
        last_time = pkt[:time]
      end

      # flush last flow
      unless current_flow.empty?
        csv << [
          global_flow_id, src_ip, src_port, dst_ip, dst_port,
          current_flow.first[:ts_raw], current_flow.last[:ts_raw],
          current_flow.size, current_flow.sum { |x| x[:size] }
        ]
        global_flow_id += 1
      end
    end
  end
end

if __FILE__ == $0
  script_dir = File.expand_path(File.dirname(__FILE__))
  input_file = File.join(script_dir, "../packet-traces/packets.csv")

  FLOW_GAPS.each do |gap|
    output_file = File.join(script_dir, "flows_#{gap}.csv")
    extract_flows(input_file, output_file, gap)
    puts "Generated #{output_file}"
  end
end
