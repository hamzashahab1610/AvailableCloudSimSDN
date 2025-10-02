import gymnasium as gym
import numpy as np
from algorithms.VNFPlacement import (
    TradeoffAwareVNFPlacement,
)


class BackupVNFPlacementEnv(gym.Env):
    def __init__(self, system):
        super(BackupVNFPlacementEnv, self).__init__()
        self.tradeoff_aware = TradeoffAwareVNFPlacement.TradeoffAwareVNFPlacement(
            system
        )
        self.system = self.tradeoff_aware.placement()
        self.candidate_nodes = sorted(
            self.system.nodes,
            key=lambda node: self.tradeoff_aware.get_node_score(node),
            reverse=True,
        )

        node_features = 5
        system_features = 2
        self.observation_space = gym.spaces.Box(
            low=0,
            high=np.inf,
            shape=(len(self.candidate_nodes) * node_features + system_features,),
            dtype=np.float32,
        )

        self.action_space = gym.spaces.MultiDiscrete(
            [3] * sum(len(sfc.vnfs) for sfc in self.system.sfcs)
        )

    def reset(self, seed=None):
        np.random.seed(seed)

        self.system.reset()
        self.tradeoff_aware.placement()

        return self._get_obs(), {}

    def _get_obs(self):
        obs = []

        for node in self.candidate_nodes:
            obs.extend(
                [
                    node.cpu,
                    node.ram,
                    node.storage,
                    node.availability,
                    node.carbon_footprint,
                ]
            )

        obs.extend(
            [
                self.system.availability,
                self.system.carbon_footprint,
            ]
        )

        return np.array(obs, dtype=np.float32)

    def step(self, action):
        reward = 0
        idx = 0

        # Add 1 to the number of redundant instances to account for the primary instance
        action = [a + 1 for a in action]

        print("Solution:", action)
        candidate_nodes = self.candidate_nodes.copy()

        for sfc in self.system.sfcs:
            primary_vnfs = [vnf for vnf in sfc.vnfs if vnf.backup_of == 0]

            for vnf in primary_vnfs:
                # Safely get number of redundant instances
                if idx >= len(action):
                    break

                redundant_instances = int(action[idx])
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

        return self._get_obs(), reward, True, True, {"system": self.system}

    def render(self, mode="human"):
        pass
