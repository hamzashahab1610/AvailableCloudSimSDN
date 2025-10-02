import sys, pickle, os,config


def find_backup_placement(original_vm_name):
    try:
        # Store the original working directory
        original_cwd = os.getcwd()
        
        # Change to the directory where this script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(script_dir)

        pkl_path = config.get_system_pkl_path()
        placements_path = config.get_placements_path()

        vnf_placements = {}
        if os.path.exists(placements_path):
            with open(placements_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        parts = line.split(",")
                        if len(parts) == 2:
                            vnf_name, host = parts
                            vnf_placements[vnf_name] = host

        with open(
            pkl_path,
            "rb",
        ) as file:
            system = pickle.load(file)

        base_vm_name = original_vm_name

        if "-backup-" in original_vm_name:
            base_vm_name = original_vm_name.split("-backup-")[0]

        original_vnf = None
        original_sfc = None

        for sfc in system.sfcs:
            for vnf in sfc.vnfs:
                if vnf.name == original_vm_name:
                    original_vnf = vnf
                    original_sfc = sfc
                    break
            if original_vnf:
                break

        if not original_vnf:
            raise ValueError(f"Original VNF {original_vm_name} not found in system")

        backup_num = 0

        while f"{base_vm_name}-backup-{backup_num}" in vnf_placements:
            backup_num += 1

        backup_name = f"{base_vm_name}-backup-{backup_num}"

        success, remaining_nodes = system.place_redundant_vnf(
            original_vnf,
            1,
            system.candidate_nodes,
            original_sfc,
            backup_num,
        )

        if success:
            system.candidate_nodes = remaining_nodes

            backup_vnf = next(
                vnf
                for vnf in original_sfc.vnfs
                if vnf.name == f"{base_vm_name}-backup-{backup_num}"
            )

            with open(
                pkl_path,
                "wb",
            ) as file:
                pickle.dump(system, file)

            with open(placements_path, "a") as f:
                f.write(f"{backup_name},{backup_vnf.node.name}\n")

            # Restore the original working directory
            os.chdir(original_cwd)

            return backup_name, backup_vnf.node.name
        else:
            # Restore the original working directory
            os.chdir(original_cwd)

            raise RuntimeError("Failed to find suitable host for backup VM")

    except FileNotFoundError:
        print(f"Error: system_simulator.pkl not found at {pkl_path}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)
    finally:
        # Make sure we restore the original working directory even if an exception occurs
        if 'original_cwd' in locals():
            os.chdir(original_cwd)


def main():
    if len(sys.argv) != 2:
        print(
            "Error: Usage: python generate_backup_placement.py <original_vm_name>",
            file=sys.stderr,
        )
        sys.exit(1)

    original_vm_name = sys.argv[1]

    try:
        backup_name, host_name = find_backup_placement(original_vm_name)
        print(f"{backup_name},{host_name}", flush=True)
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
