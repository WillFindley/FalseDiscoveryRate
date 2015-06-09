% function [FDRP pi0 Coeffs P yCDF Y] = cdfFit(P,FDR)
%
% cdfFit takes a vector of p-values with a desired false discovery rate
% (FDR) and outputs the significance threshold p-value as well as
% information about the p-value model.
%
% Inputs:
% P -- a vector of p-values
% FDR -- desired false discovery rate
%
% Outputs:
% FDRP -- the p-value cut-off for desired FDR
% pi0 -- the estimated fraction of p-values that are a result of chance 
% Coeffs -- a matrix containing the coefficients of the incomplete regularized
% beta function
% P -- the sorted list of empirical p-values (downsampled to 1000 p-values
% if original input > 1000 p-values)
% yCDF -- the empirical p-value cdf for P
% Y -- the estimated model's cdf for P

function [FDRP pi0 Coeffs P yCDF Y] = cdfFit(P,FDR)

P = sort(P,'descend');%sorts p-values into descending order
if length(P)>1000%if there are more than 1000 p-values, downsamples to 1000 p-values
    P = P(1:round(length(P)/1000):length(P));
end
[yCDF,P] = cdfcalc(P);%creates an empiricial cdf for the p-values
yCDF(1) = [];%deletes first element of yCDF, which was automatically added and set to zero

StepSizeA = .1;%defines step-size of coefficient A for the search algorithm
StepSizeB = .1;%defines step-size of coefficient B for the search algorithm
StepSizeC = 1;%defines step-size of coefficient C for the search algorithm

% coefficient A is the fraction of p-values that are a result of chance,
% coefficients B and C are the coefficients of the incomplete regularized
% beta function, which are chosen such that the probability density
% function of the beta distribution is strictly convex
A = [0:StepSizeA/2:1];%defines matrix A, which increases from 0 to 1 by steps of a size defined above
B = [0:StepSizeB/2:1];%defines matrix B, which increases from 0 to 1 by steps of a size defined above
C = [2:StepSizeC/2:12];%defines matrix C, which increases from 2 to 12 by steps of a size defined above

while StepSizeA>=0.0001%creates a while loop that guarantees coefficients with four significant digits
    
    NumCombs = zeros(length(A)*length(B)*length(C),3);%creates NumCombs, a matrix of zeros that will contain all possible combinations of coefficients A, B, and C
    
    %manipulates matrices A, B, and C to fill NumCombs
    NumCombs(:,1) = reshape(repmat(A,[length(C)*length(B),1]),[length(A)*length(B)*length(C),1]);
    NumCombs(:,2) = reshape(repmat(B,[length(C),1,length(A)]),[length(A)*length(B)*length(C),1]);
    NumCombs(:,3) = repmat(C', [length(A)*length(B),1]);
    
    SqErr = zeros(length(NumCombs),1);%creates a matrix of zeros that will record the error of each distribution fitting attempt
    
    for WhichY = 1:length(NumCombs)%creates while loop to test each set of coefficients
        TestCoeffs = NumCombs(WhichY,:);%selects one combination of coefficients from NumCombs
        SqErr(WhichY) = sum(((P.^(1-2*min(0.5,mean(P)))).*(MixBetaCDF(TestCoeffs,P) - yCDF)).^2);%records the weighted error of tested coefficients
        %SqErr(WhichY) = sum(((MixBetaCDF(TestCoeffs,P) - yCDF)).^2);
    end
    
    [LsSq,I] = min(SqErr)%records index of coefficients with minimum error
    
    if(NumCombs(I,3) == max(C)) & (StepSizeC == 1)%if still on largest grid-resolution, move search grid for coefficient C until the minimum error is within it
        C = C + max(C) - min(C);%defines new range for C
    else
        %increase resolution of search grid by a factor of 10
        StepSizeA = StepSizeA/10;
        StepSizeB = StepSizeB/10;
        StepSizeC = StepSizeC/10;
        
        %defines new, smaller ranges for matrices A, B and C
        A = max((NumCombs(I,1)-10*StepSizeA),0):StepSizeA:min((NumCombs(I,1)+10*StepSizeA),1);
        B = max((NumCombs(I,2)-10*StepSizeB),0):StepSizeB:min((NumCombs(I,2)+10*StepSizeB),1);
        C = max((NumCombs(I,3)-10*StepSizeC),2):StepSizeC:(NumCombs(I,3)+10*StepSizeC);
    end
    
    Coeffs = NumCombs(I,:)%sets final chosen coefficients to those which were found to be optimal for the used step-size
end

pi0 = Coeffs(1);%sets pi0, the fraction of p-values that are a result of chance

Y = MixBetaCDF(Coeffs,P);%generates the model's p-value cdf using the above chosen coefficients and the function MixBetaCDF

try%true for all 0<FDRP<1
    FDRP = fzero(@(FDRP)(((pi0*FDRP) / MixBetaCDF(Coeffs,FDRP)) - FDR),[eps,1]);%locates the appropriate FDRP between 0 and 1
catch %FDRP is either 0 or 1
    FDRP = 1;%assume FDRP is 1
    %if FDR is always greater than target FDR, set FDRP to 0
    %otherwise, FDRP is known to be 1
    if (((pi0*FDRP) / MixBetaCDF(Coeffs,FDRP)) - FDR)>0
        FDRP = 0;
    end
end

Coeffs(1) = [];%remove pi0, leaving only the two beta function coefficients

%Using our model mixing the uniform distribution cdf and the incomplete regularized beta
%function (i.e. the beta distribution's cdf), we calculate the mixed
%model's value for each p-value
function Y = MixBetaCDF(Coeffs,P)

Y= Coeffs(1)*P+(1-Coeffs(1))*betainc(P,Coeffs(2),Coeffs(3));
