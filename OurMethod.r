############################################################################################
#
# function [FDRP pi0 Coeffs P yCDF Y] = cdfFit(P,FDR)
#
# cdfFit takes a vector of p-values with a desired false discovery rate
# (FDR) and outputs the significance threshold p-value as well as
# information about the p-value model.
#
# Inputs:
# P -- a vector of p-values
# FDR -- desired false discovery rate
#
# Outputs:
# FDRP -- the p-value cut-off for desired FDR
# pi0 -- the estimated fraction of p-values that are a result of chance
# Coeffs -- a matrix containing the coefficients of the incomplete regularized
# beta function
# P -- the sorted list of empirical p-values (downsampled to 1000 p-values
# if original input > 1000 p-values)
# yCDF -- the empirical p-value cdf for P
# Y -- the estimated model's cdf for P
#
############################################################################################

numPs <- 10
numData <- 20
StoN <- 0.15

P <- matrix(1,numPs,1)
for (i in 1:numPs) {
    data <- matrix(StoN,numData,1) + rnorm(numData)
    statData <- wilcox.test(data)
    P[i] <- statData$p.value
}

OurMethod <- function(P, FDR) {

	P <- matrix(P,length(P),1)
	P <- matrix(P[rev(order(P))])
	if (length(P) > 1000) P <- approx(P,n=1000)
	Fn <- ecdf(P)
	yCDF <- Fn(P)

	stepSizeA <- 0.1
	stepSizeB <- 0.1
	stepSizeC <- 1
	tol <- 10^-15

	A <- t(matrix(seq(0,1,by=stepSizeA/2)))
	B <- t(matrix(seq(0,1,by=stepSizeB/2)))
	C <- t(matrix(seq(2,12,by=stepSizeC/2)))

	while (stepSizeA >= 0.0001) {

	      numGridPos <- ncol(A)*ncol(B)*ncol(C)
	      numCombs <- matrix(0,numGridPos,3)

	      numCombs[,1] <- matrix(kronecker(matrix(1,ncol(C)*ncol(B),1),A),ncol=numGridPos)
	      numCombs[,2] <- matrix(kronecker(matrix(1,ncol(C),1,ncol(A)),B),ncol=numGridPos)
	      numCombs[,3] <- kronecker(matrix(1,ncol(A)*ncol(B),1),t(C))

	      sqErr <- matrix(0,nrow(numCombs),1)

	      for (whichY in 1:nrow(numCombs)) {
	      	  testCoeffs <- numCombs[whichY,]
		  sqErr[whichY] <- sum(((P ^ (1-2*pmin(0.5,apply(P,2,mean)))) * (mixBetaCDF(testCoeffs,P) - yCDF)) ^ 2)
#		  sqErr[whichY] <- sum((mixBetaCDF(testCoeffs,P) - yCDF) ^ 2)
	      }

	      lsSqInd <- which.min(sqErr)

	      if ((numCombs[lsSqInd,3] == max(C)) && (stepSizeC == 1)) {
	      	  C <- C + max(C) - min(C)
	      } else {
	      	  stepSizeA = stepSizeA/10;
        	  stepSizeB = stepSizeB/10;
        	  stepSizeC = stepSizeC/10;

		  A <- t(matrix(seq(max(numCombs[lsSqInd,1]-10*stepSizeA,0),min(numCombs[lsSqInd,1]+10*stepSizeA,1),by=stepSizeA)))
        	  B <- t(matrix(seq(max(numCombs[lsSqInd,2]-10*stepSizeB,0),min(numCombs[lsSqInd,2]+10*stepSizeB,1),by=stepSizeB)))
        	  C <- t(matrix(seq(max(numCombs[lsSqInd,3]-10*stepSizeC,2),numCombs[lsSqInd,3]+10*stepSizeC,by=stepSizeC)))

	      }

	      coeffs <- matrix(numCombs[lsSqInd,])

	}

	pi0 <- coeffs[1]
	y <- mixBetaCDF(matrix(coeffs),P)
	FDRP <- fzero(matrix(coeffs), FDR, tol)
	if (FDRP <= tol) FDRP <- 0

	return(c(FDRP, pi0, coeffs, P, yCDF, y))

}

mixBetaCDF <- function(coeffs, P) {

	incBeta <- pbeta(P,coeffs[2],coeffs[3])

	return(coeffs[1]*P+(1-coeffs[1])*incBeta)

}

fzero <- function(coeffs, FDR, tol) {

        pi0 <- coeffs[1]
	FDRP <- 0.5
	stepSize <- 0.25
	if(((pi0 / mixBetaCDF(coeffs,1)) - FDR) <= 0) {
		FDRP <- 1
	} else {
		while (stepSize > tol) {
	      	     diffZero <- ((pi0*FDRP) / mixBetaCDF(coeffs,FDRP)) - FDR
	      	     ifelse(diffZero > 0, FDRP <- FDRP - stepSize, FDRP <- FDRP + stepSize)
	      	     stepSize <- stepSize/2
		}
	}

	return(FDRP)

}