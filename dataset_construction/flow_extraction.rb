require 'csv'
require 'optparse'

# Flow gaps in microseconds
FLOW_GAPS = [500, 5000, 10000]
DEFAULT_IP = 192.168.60.81

# Parse "HH:MM:SS.ssssss" -> float seconds
def parse_time(ts)
  h, m, s = ts.split(":")
  h.to_i * 3600 + m.to_i * 60 + s.to_f
end

# Binary-search helper: given a sorted timeline [[t0, v0], [t1, v1], ...],
# return cumulative value at time <= t (or 0 if none).
def cumulative_at(timeline, t)
  return 0 if timeline.nil? || timeline.empty?
  lo, hi = 0, timeline.length - 1
  ans = -1
  while lo <= hi
    mid = (lo + hi) / 2
    if timeline[mid][0] <= t
      ans = mid
      lo = mid + 1
    else
      hi = mid - 1
    end
  end
  return 0 if ans < 0
  timeline[ans][1]
end

def extract_flows(input_file, output_file, flow_gap_us, vantage_ip)
  flow_gap_s = flow_gap_us.to_f / 1_000_000.0
  global_flow_id = 0

  groups = Hash.new { |h, k| h[k] = [] }
  events = []

  CSV.foreach(input_file, headers: true) do |row|
    t = parse_time(row["timestamp"])
    src_ip = row["source_ip"]
    dst_ip = row["destination_ip"]
    size = row["packet_size"].to_i

    key = [src_ip, row["source_port"], dst_ip, row["destination_port"]]
    groups[key] << { time: t, size: size, ts_raw: row["timestamp"] }

    events << { t: t, src: src_ip, dst: dst_ip, size: size }
  end

  # Build cumulative incoming/outgoing timelines per IP
  events.sort_by! { |e| e[:t] }
  outgoing_tl = Hash.new { |h, k| h[k] = [] }
  incoming_tl = Hash.new { |h, k| h[k] = [] }
  out_cum = Hash.new(0)
  in_cum = Hash.new(0)

  events.each do |e|
    out_cum[e[:src]] += e[:size]
    in_cum[e[:dst]] += e[:size]
    outgoing_tl[e[:src]] << [e[:t], out_cum[e[:src]]]
    incoming_tl[e[:dst]] << [e[:t], in_cum[e[:dst]]]
  end

  CSV.open(output_file, "w") do |csv|
    csv << [
      "flow_id", "src_ip", "src_port", "dst_ip", "dst_port",
      "start_time", "end_time", "packet_count", "total_size",
      "gap_us", "neworkin", "neworkout"
    ]

    groups.each do |(src_ip, src_port, dst_ip, dst_port), packets|
      packets.sort_by! { |p| p[:time] }

      current_flow = []
      last_pkt_time = nil
      prev_flow_end = nil

      packets.each do |pkt|
        if last_pkt_time && (pkt[:time] - last_pkt_time > flow_gap_s)
          start_t  = current_flow.first[:time]
          end_t    = current_flow.last[:time]
          start_ts = current_flow.first[:ts_raw]
          end_ts   = current_flow.last[:ts_raw]
          gap_us   = prev_flow_end.nil? ? 0 : ((start_t - prev_flow_end) * 1_000_000).round

          # Determine the reference IP
          ref_ip = vantage_ip || src_ip
          net_in  = cumulative_at(incoming_tl[ref_ip], start_t - 1e-12)
          net_out = cumulative_at(outgoing_tl[ref_ip], start_t - 1e-12)

          csv << [
            global_flow_id, src_ip, src_port, dst_ip, dst_port,
            start_ts, end_ts, current_flow.size, current_flow.sum { |x| x[:size] },
            gap_us, net_in, net_out
          ]

          global_flow_id += 1
          prev_flow_end = end_t
          current_flow = []
        end

        current_flow << pkt
        last_pkt_time = pkt[:time]
      end

      unless current_flow.empty?
        start_t  = current_flow.first[:time]
        end_t    = current_flow.last[:time]
        start_ts = current_flow.first[:ts_raw]
        end_ts   = current_flow.last[:ts_raw]
        gap_us   = prev_flow_end.nil? ? 0 : ((start_t - prev_flow_end) * 1_000_000).round

        ref_ip = vantage_ip || src_ip
        net_in  = cumulative_at(incoming_tl[ref_ip], start_t - 1e-12)
        net_out = cumulative_at(outgoing_tl[ref_ip], start_t - 1e-12)

        csv << [
          global_flow_id, src_ip, src_port, dst_ip, dst_port,
          start_ts, end_ts, current_flow.size, current_flow.sum { |x| x[:size] },
          gap_us, net_in, net_out
        ]
        global_flow_id += 1
      end
    end
  end
end

if __FILE__ == $0
  script_dir = File.expand_path(File.dirname(__FILE__))
  input_file = File.join(script_dir, "../trace_collector/packet_traces/packets.csv")

  options = {}
  OptionParser.new do |opts|
    opts.banner = "Usage: ruby flow_extractor.rb [options]"
    opts.on("--ip IP", "Specific vantage IP address") { |v| options[:ip] = v }
  end.parse!

  vantage_ip = options[:ip] || DEFAULT_IP
  puts "Using vantage IP: #{vantage_ip || '(per-flow source_ip)'}"

  FLOW_GAPS.each do |gap|
    output_file = File.join(script_dir, "flows_#{gap}.csv")
    extract_flows(input_file, output_file, gap, vantage_ip)
    puts "Generated #{output_file}"
  end
end
