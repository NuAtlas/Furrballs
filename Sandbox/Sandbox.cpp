// Sandbox.cpp : Source file for your target.
//

#include "Sandbox.h"

int main() {
	FurrBall* fb = FurrBall::CreateBall("TestDB");
	if (!fb) {
		std::cerr << "Error: Furrball has not initialized";
		return -1;
	}

}
