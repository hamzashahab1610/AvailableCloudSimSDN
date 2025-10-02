import numpy as np
from typing import List, Tuple


class PSOBackupVNFPlacement:
    def __init__(
        self, policy, num_particles=50, max_iterations=100, w=0.7, c1=1.5, c2=1.5
    ):
        self.policy = policy
        self.system = self.policy.placement()
        self.candidate_nodes = sorted(
            self.system.nodes,
            key=lambda node: self.policy.get_node_score(node),
        )
        self.num_particles = num_particles
        self.max_iterations = max_iterations
        self.w = w
        self.c1 = c1
        self.c2 = c2
        self.dimension = sum(len(sfc.vnfs) for sfc in self.system.sfcs)

    def initialize_particles(
        self,
    ) -> Tuple[List[np.ndarray], List[np.ndarray], List[np.ndarray]]:
        particles = [
            np.random.randint(1, 3, self.dimension) for _ in range(self.num_particles)
        ]
        velocities = [np.random.rand(self.dimension) for _ in range(self.num_particles)]
        personal_best = particles.copy()
        return particles, velocities, personal_best

    def fitness(self, particle: np.ndarray) -> float:
        self.system.reset()
        self.policy.placement()

        reward = 0
        idx = 0

        print("Solution:", particle)
        candidate_nodes = self.candidate_nodes.copy()

        for sfc in self.system.sfcs:
            primary_vnfs = [vnf for vnf in sfc.vnfs if vnf.backup_of == 0]

            for vnf in primary_vnfs:
                # Safely get number of redundant instances
                if idx >= len(particle):
                    break

                redundant_instances = int(particle[idx])
                idx += 1

                if redundant_instances == 0:
                    continue

                # Try to place redundant VNFs
                success, candidate_nodes = self.system.place_redundant_vnf(
                    vnf, redundant_instances, candidate_nodes, sfc
                )

                if not success:
                    print(f"Placement failed for VNF {vnf} in SFC {sfc}")
                    reward -= 1

        if self.system.calculate_availability() < self.system.min_availability:
            print("Availability constraint violated")
            reward -= 1

        if self.system.calculate_carbon_footprint() > self.system.max_carbon_footprint:
            print("Carbon footprint constraint violated")
            reward -= 1

        reward += self.system.calculate_objective()

        print("Reward:", reward)
        print()

        return reward

    def update_velocity(
        self,
        particle: np.ndarray,
        velocity: np.ndarray,
        personal_best: np.ndarray,
        global_best: np.ndarray,
    ) -> np.ndarray:
        r1, r2 = np.random.rand(2)
        new_velocity = (
            self.w * velocity
            + self.c1 * r1 * (personal_best - particle)
            + self.c2 * r2 * (global_best - particle)
        )
        return new_velocity

    def update_position(self, particle: np.ndarray, velocity: np.ndarray) -> np.ndarray:
        new_position = particle + velocity
        new_position = np.clip(new_position, 0, 3)
        return np.round(new_position).astype(int)

    def optimize(self) -> Tuple[np.ndarray, float]:
        particles, velocities, personal_best = self.initialize_particles()
        global_best = max(particles, key=self.fitness)
        global_best_fitness = self.fitness(global_best)

        for _ in range(self.max_iterations):
            for i in range(self.num_particles):
                fitness = self.fitness(particles[i])

                if fitness > self.fitness(personal_best[i]):
                    personal_best[i] = particles[i]

                if fitness > global_best_fitness:
                    global_best = particles[i]
                    global_best_fitness = fitness

                velocities[i] = self.update_velocity(
                    particles[i], velocities[i], personal_best[i], global_best
                )
                particles[i] = self.update_position(particles[i], velocities[i])

        return global_best, global_best_fitness

    def apply_solution(self, solution: np.ndarray):
        idx = 0
        self.system.reset()
        self.policy.placement()
        self.system.solution = solution

        print("Solution:", solution)
        candidate_nodes = self.candidate_nodes.copy()

        for sfc in self.system.sfcs:
            primary_vnfs = [vnf for vnf in sfc.vnfs if vnf.backup_of == 0]

            for vnf in primary_vnfs:
                # Safely get number of redundant instances
                if idx >= len(solution):
                    break

                redundant_instances = int(solution[idx])
                idx += 1

                if redundant_instances == 0:
                    continue

                # Try to place redundant VNFs
                self.system.place_redundant_vnf(
                    vnf, redundant_instances, candidate_nodes, sfc
                )

        return self.system
